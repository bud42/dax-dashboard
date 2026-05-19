pyinstaller \
  launcher.py \
  --name DAXdashboard \
  --windowed \
  --onedir \
  --collect-all dash_bootstrap_templates \
  --collect-all daxdashboard \
  --icon=dax.ico
