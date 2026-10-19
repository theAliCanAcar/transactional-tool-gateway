/**
 * In-memory saga coordinator. Adapters own their state and persistence; this is
 * neither an OS transaction nor a durable crash-recovery service. A shared
 * instance serializes runs. Use an external durable journal for production.
 */
const copy = value => structuredClone(value);
const fail = message => { throw new Error(message); };
const fingerprint = action => JSON.stringify({tool: action.tool, input: action.input});

export class TransactionalToolGateway {
  /**
   * @param {{tools:Object, permissions?:string[], approvals?:string[], assumptionGuard?:(action:Object)=>void}} options
   * approvals contains explicit action idempotency keys; irreversible tools
   * always require one. A key cannot be reused with different tool/input data.
   */
  constructor({tools, permissions = [], approvals = [], assumptionGuard = null} = {}) {
    if (!tools || typeof tools !== 'object') throw new TypeError('Tools are required');
    this.tools = tools;
    this.permissions = new Set(permissions);
    this.approvals = new Set(approvals);
    this.assumptionGuard = assumptionGuard;
    this._journal = new Map();
    this._running = false;
  }
  /** Inspect a defensive copy of each action's state, result, and recovery. */
  get journal() { return [...this._journal.values()].map(copy); }

  /** Run [{tool,input,key}] in order. Returns COMMITTED, BLOCKED, COMPENSATED or ESCALATE. */
  async run(actions) {
    if (this._running) throw new Error('Another transaction is running');
    if (!Array.isArray(actions) || !actions.length) throw new TypeError('Actions are required');
    this._running = true;
    const active = [];
    let current = null;
    try {
      // Freeze the planned data against caller mutation across asynchronous gates.
      const plan = copy(actions);
      const seen = new Set();
      for (const action of plan) {
        if (!action || typeof action.key !== 'string' || !action.key.trim() || seen.has(action.key)) fail('Unique idempotency keys are required');
        seen.add(action.key);
        const tool = this.tools[action.tool];
        if (!tool || typeof tool.execute !== 'function' || typeof tool.prepare !== 'function' || typeof tool.precondition !== 'function' || typeof tool.postcondition !== 'function' || typeof tool.validateInput !== 'function' || typeof tool.validateOutput !== 'function' || typeof tool.reconcile !== 'function' || !Array.isArray(tool.permissions) || typeof tool.reversible !== 'boolean') fail('Incomplete tool execution policy');
        if (tool.reversible && typeof tool.compensate !== 'function') fail('Reversible tool needs compensation');
        if (tool.validateInput(action.input) !== true) fail('Input schema rejected');
        if (!tool.permissions.every(permission => this.permissions.has(permission))) fail('Permission denied');
        if ((!tool.reversible || tool.approvalRequired) && !this.approvals.has(action.key)) fail('Explicit approval required');
        await this.assumptionGuard?.({...action, irreversible: !tool.reversible});
        const previous = this._journal.get(action.key);
        if (previous && previous.fingerprint !== fingerprint(action)) fail('Idempotency key conflict');
        if (previous && previous.status !== 'COMMITTED') fail('Action key needs reconciliation or a new key');
      }
      for (const action of plan) {
        const tool = this.tools[action.tool];
        const previous = this._journal.get(action.key);
        if (previous) {
          const observed = await tool.reconcile(action.input, previous.before, previous.result);
          if (observed !== 'applied') fail('Previously committed action no longer reconciles');
          continue;
        }
        const before = await tool.prepare(action.input);
        if (await tool.precondition(action.input, before) !== true) fail('Precondition failed');
        // Gates run again immediately before commit because earlier actions may change context.
        if (!tool.permissions.every(permission => this.permissions.has(permission))) fail('Permission revoked');
        if ((!tool.reversible || tool.approvalRequired) && !this.approvals.has(action.key)) fail('Approval revoked');
        await this.assumptionGuard?.({...action, irreversible: !tool.reversible});
        current = {key:action.key, tool:action.tool, input:copy(action.input), fingerprint:fingerprint(action), before:copy(before), status:'COMMITTING'};
        this._journal.set(action.key, current);
        active.push(current);
        current.result = copy(await tool.execute(action.input, before));
        if (tool.validateOutput(current.result) !== true) fail('Output schema rejected');
        if (await tool.postcondition(action.input, before, current.result) !== true) fail('Postcondition failed');
        current.status = 'COMMITTED';
        current = null;
      }
      return {status:'COMMITTED', journal:this.journal};
    } catch (error) {
      let escalated = false;
      for (const entry of [...active].reverse()) {
        const tool = this.tools[entry.tool];
        try {
          const observed = await tool.reconcile(entry.input, entry.before, entry.result);
          entry.reconciled = observed;
          if (observed === 'not-applied') { entry.status = 'NOT_APPLIED'; continue; }
          if (observed !== 'applied') { entry.status = 'AMBIGUOUS'; escalated = true; continue; }
          if (!tool.reversible) { entry.status = 'IRREVERSIBLE'; escalated = true; continue; }
          if (await tool.compensate(entry.input, entry.before, entry.result) !== true) fail('Compensation did not verify restoration');
          if (await tool.reconcile(entry.input, entry.before, entry.result) !== 'not-applied') fail('Restored state did not reconcile');
          entry.status = 'COMPENSATED';
        } catch (recoveryError) {
          entry.status = 'COMPENSATION_FAILED';
          entry.recoveryError = recoveryError.message;
          escalated = true;
        }
      }
      return {status:escalated ? 'ESCALATE' : active.length ? 'COMPENSATED' : 'BLOCKED', error:error.message, journal:this.journal};
    } finally { this._running = false; }
  }

  /** Observe adapter state without retrying a possibly committed side effect. */
  async reconcile(key) {
    if (this._running) throw new Error('Cannot reconcile during a transaction');
    const entry = this._journal.get(key);
    if (!entry) throw new Error('Unknown action key');
    const observed = await this.tools[entry.tool].reconcile(entry.input, entry.before, entry.result);
    return {key, observed, status:entry.status};
  }
}

/**
 * Three deterministic local test doubles: virtual temporary files, a record,
 * and an external-operation outbox simulation. No filesystem/network writes.
 * failAfterWrite mimics a lost response after a successful file write.
 */
export function createSimulatedTools() {
  const state = {files:{}, record:{value:0}, external:[]};
  const same = (left, right) => JSON.stringify(left) === JSON.stringify(right);
  const writeFile = {
    reversible:true, permissions:['files:write'], approvalRequired:false,
    validateInput:input => !!input && typeof input.path === 'string' && /^\/tmp\/[a-zA-Z0-9_-]+\.txt$/.test(input.path) && typeof input.content === 'string',
    validateOutput:output => !!output && output.written === true,
    prepare:input => ({exists:Object.hasOwn(state.files,input.path), content:state.files[input.path]}),
    precondition:() => true,
    execute:input => { state.files[input.path]=input.content; if(input.failAfterWrite)throw new Error('Simulated response lost after write'); return {written:true}; },
    postcondition:input => state.files[input.path] === input.content,
    reconcile:(input,before) => (before.exists ? state.files[input.path]===before.content : !Object.hasOwn(state.files,input.path)) ? 'not-applied' : state.files[input.path] === input.content ? 'applied' : 'unknown',
    compensate:(input,before) => { if(input.failCompensation)throw new Error('Simulated compensation failure'); if(before.exists)state.files[input.path]=before.content;else delete state.files[input.path]; return true; },
  };
  const updateRecord = {
    reversible:true, permissions:['record:write'], approvalRequired:false,
    validateInput:input => !!input && Number.isFinite(input.value), validateOutput:output => !!output && Number.isFinite(output.value),
    prepare:() => copy(state.record), precondition:() => true,
    execute:input => {state.record.value=input.value; return copy(state.record);},
    postcondition:input => !input.failPostcondition && state.record.value===input.value,
    reconcile:(input,before) => same(state.record,before) ? 'not-applied' : state.record.value===input.value ? 'applied' : 'unknown',
    compensate:(_input,before) => {state.record=copy(before);return true;},
  };
  const externalOperation = {
    reversible:false, approvalRequired:true, permissions:['external:trigger'],
    validateInput:input => !!input && typeof input.operation==='string' && input.operation.trim().length>0,
    validateOutput:output => !!output && output.accepted===true,
    prepare:() => ({count:state.external.length}), precondition:() => true,
    execute:input => {state.external.push(input.operation);return {accepted:true};},
    postcondition:input => !input.failPostcondition,
    reconcile:(input,before) => state.external.length===before.count ? 'not-applied' : state.external[before.count]===input.operation ? 'applied' : 'unknown',
  };
  return {state, tools:{writeFile, updateRecord, externalOperation}};
}
