# Hexa Backup

A cool distibuted backup system, with CLI and web ui.
I make this to backup all my personal data (1.4TB of unique data, duplicated on three different geographical locations).

It goes hand in hand with other services for indexation, better UI, mp3 and media streaming...

## Systemd service installation

```bash
sudo cp hexa-backup.service /etc/systemd/system/
sudo systemctl start hexa-backup
```