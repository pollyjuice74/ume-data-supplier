# Oracle Always Free deployment: Ethereum wallet feed

This deploys only the independent supplier. The analytics dashboard is not exposed.

## 1. Create the VM

In Oracle Cloud create a **VM.Standard.A1.Flex** instance in the Always Free eligible home region:

- Image: Ubuntu 24.04 Minimal aarch64
- Shape: Ampere A1, 1 OCPU, 6 GB RAM (or any remaining Always Free allocation)
- Boot volume: 50 GB, default encryption
- Networking: public IPv4, SSH key authentication, ingress TCP 22 from the operator IP only
- Do not expose port 8090 directly. Put HTTPS Caddy/Nginx or an Oracle Load Balancer in front and allow 443 only.
- Reserve the public IPv4 so the DNS origin remains stable.

Create an `A` record such as `supplier.example.com`, obtain TLS, and reverse proxy only the supplier origin to `127.0.0.1:8090`. Never proxy the UME backend analytics port.

## 2. Install

From the workstation (replace values):

```bash
ssh ubuntu@ORACLE_IP 'sudo apt-get update && sudo apt-get install -y git'
ssh ubuntu@ORACLE_IP 'git clone --branch feature/production-wallet-feed https://github.com/pollyjuice74/ume-data-supplier.git /tmp/ume-data-supplier'
ssh ubuntu@ORACLE_IP 'sudo /tmp/ume-data-supplier/scripts/deploy_oracle_arm64.sh'
ssh ubuntu@ORACLE_IP 'sudoedit /etc/ume-data-supplier.env'
ssh ubuntu@ORACLE_IP 'sudo /tmp/ume-data-supplier/scripts/deploy_oracle_arm64.sh'
```

Set a stable HTTPS `SUPPLIER_PUBLIC_URL`, a dedicated Ethereum RPC URL, a random session secret, chain/RPC/FeedAccess values from the **same test network**, and the seller receiving wallet. `SUPPLIER_SELLER_PRIVATE_KEY` is optional and should remain blank: origin ownership can be paired by signing in MetaMask. Buyer private keys never enter this service.

Check it:

```bash
ssh ubuntu@ORACLE_IP 'systemctl is-enabled ume-data-supplier; systemctl is-active ume-data-supplier'
ssh ubuntu@ORACLE_IP 'sudo journalctl -u ume-data-supplier -f'
curl -fsS https://supplier.example.com/health | python3 -m json.tool
curl -fsS https://supplier.example.com/feeds/ethereum-wallet-trading-v1 | python3 -m json.tool
curl -fsS https://supplier.example.com/payments/status | python3 -m json.tool
```

`Restart=always`, SQLite WAL, the canonical-block checkpoint, deterministic event IDs and unique constraints provide restart recovery. Retention and maximum event settings bound local storage. Back up the `.sqlite3` file only if historical continuity beyond that bounded window is required.

## 3. Register in KPM

Use the existing **Sell Data** page while connected as the seller wallet:

1. Enter `https://supplier.example.com` and discover `ethereum-wallet-trading-v1`.
2. Sign the exact origin-ownership message. The signature proves control of the connected wallet; it is not a transaction.
3. Choose the test-USDC price, one-hour access duration and seller receiving wallet.
4. Sign the listing authorization and publish. KPM calls `POST /v1/data/external-feed-listings` idempotently and validates the supplier origin, owner signature, live sample and schema.
5. The configured FeedAccess owner must configure the same feed ID, price, duration and seller. On a public testnet without an admin key in KPM, this is a manual prerequisite.

The supplier's `/payments/status` reads `paymentToken()`, `treasury()` and `protocolFeeBps()` from the configured FeedAccess. Continue only when it reports the expected 250 bps. The treasury address shown there is the protocol-fee collection wallet. No private key for it belongs on the supplier VM.

## 4. Test purchase and consumption

1. Open **Explore Data**, select **Live Ethereum Wallet Trading Feed**, and inspect its real sample/schema/status.
2. Connect MetaMask on the configured test network and use test USDC only.
3. Approve the exact token amount and call Buy. FeedAccess atomically sends 97.5% to the seller wallet and 2.5% to its configured treasury, then records `expiresAt`.
4. Sign the supplier authentication challenge, request `POST /feeds/ethereum-wallet-trading-v1/stream-ticket`, and consume `/feeds/ethereum-wallet-trading-v1/sse?ticket=...`.
5. Use the same bearer session to download `.csv`, `.json`, or `.parquet` history. An unpaid wallet must receive 403.

For a terminal SSE check, pass the short-lived ticket without logging it:

```bash
read -rsp 'ticket: ' TICKET; echo; python scripts/buyer_sse.py --url https://supplier.example.com --ticket "$TICKET" --count 5
```

## Production-money blockers

- The checked-in evidence verifies only local Anvil/test-USDC behavior. It does not identify a verified real-USDC deployment.
- A production chain, canonical USDC address, verified FeedAccess bytecode, owner, treasury wallet, 250 bps setting, funded gas wallet and completed contract/security review are still required.
- Marketplace production mode must not be `intake_only`, and production listing terms must be independently verified.
- TLS, DNS, RPC account/rate limits, monitoring/alerts, backups and applicable data-sale/legal obligations require external account configuration.
- Do not describe test USDC as revenue. No real-money contract is deployed or funded by these instructions.
