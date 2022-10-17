FROM python:3.8-slim-buster

# Install prereqs for dashboard
RUN pip install pycap==2.1.0
RUN pip install flask==2.1.3
RUN pip install pandas dax dash dash_auth
RUN pip install kaleido
RUN pip uninstall -y werkzeug && pip install -v https://github.com/pallets/werkzeug/archive/refs/tags/2.0.1.tar.gz
RUN pip install xlsxwriter

# Copy over dashboard code
COPY dashboard /opt/dashboard

# Enable external access
EXPOSE 8050

# Set our working directory
WORKDIR /opt/dashboard

# Set out our entry point to run the app
CMD ["python", "index.py"]
