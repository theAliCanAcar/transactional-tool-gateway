# Transactional Tool Gateway

Prepare, commit, reconcile, and compensate local tool actions.

Standalone ES module. No npm install required. Works in **Node.js 20+** and modern browsers (serve over HTTP for browser demos).

## Install / import

Copy the files into your project, or clone this repository:

```js
import {TransactionalToolGateway,createSimulatedTools} from './transactional-tool-gateway.js';
const {tools,state}=createSimulatedTools();
const gateway=new TransactionalToolGateway({tools,permissions:['files:write','record:write']});
const result=await gateway.run([
  {tool:'writeFile',key:'draft-1',input:{path:'/tmp/draft.txt',content:'Approved draft'}},
  {tool:'updateRecord',key:'record-1',input:{value:42,failPostcondition:true}}
]);
console.log(result.status, state, gateway.journal); // COMPENSATED, original state restored
// Use case: draft creation and metadata update must both pass their checks.
```

## Use cases

### 1. Undo a partially completed local workflow

- **Scenario:** An agent writes a virtual file and updates a record, then a later step fails.
- **Outcome:** Compensate reversible committed actions in reverse order and record the result.

### 2. Gate an irreversible external action

- **Scenario:** A simulated external operation needs explicit approval.
- **Outcome:** Block an unapproved commit; escalate if a committed action cannot safely be compensated.

## Exports

- `TransactionalToolGateway`
- `createSimulatedTools`

## Notes

- Keep API keys and secrets out of this module and out of browser storage.
- Treat verification callbacks, ledger writers, and permission checks as trusted application code.
- This library is intentional local reliability plumbing, not a claim of cryptographic attestation unless stated in the source comments.

## License

MIT
