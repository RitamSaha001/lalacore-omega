# Run Refinement When Laptop Is Off

Local jobs cannot run while a laptop is powered off.  
Use an always-on machine (EC2/VM) and run this pipeline there.

## 1) One-shot run (manual)

```bash
cd /opt/lalacore_omega
source venv/bin/activate
REQUIRE_AI_ALL=1 MAX_AI_ROWS=0 TOKEN_BUDGET=0 \
AI_MAX_RETRIES=3 AI_RETRY_DELAY_S=3 AI_TIMEOUT_S=20 \
./tools/run_refine_pipeline.sh data/app/import_question_bank.json
```

## 2) Continuous service (systemd)

1. Copy service file:
```bash
sudo cp deploy/systemd/lalacore-refine.service /etc/systemd/system/
```

2. Update `User=` and `WorkingDirectory=` in the service file if needed.

3. Enable and start:
```bash
sudo systemctl daemon-reload
sudo systemctl enable lalacore-refine
sudo systemctl start lalacore-refine
```

4. Logs/progress:
```bash
sudo journalctl -u lalacore-refine -f
cat /opt/lalacore_omega/data/app/repair_report_layer4.progress.live.json
```

## 3) Final output files

- `data/app/import_question_bank_final.live.json`
- Active bank auto-published to: `data/app/import_question_bank.json`

## 4) Safety locks enabled

- AI mandatory review for all rows (`--require-ai-check-all`)
- Retry/backoff + provider rotation hooks (LalaCore `ai_chat`)
- Similarity lock so AI cannot rewrite entire question
- Broken/unusable rows dropped from final published bank
