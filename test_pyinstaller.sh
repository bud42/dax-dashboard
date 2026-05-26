pyinstaller \
launcher.py \
--name DAXdashboard \
--onedir \
--console \
--windowed \
--collect-all dash_bootstrap_templates \
--collect-all daxdashboard \
--icon=icons/dax.ico
