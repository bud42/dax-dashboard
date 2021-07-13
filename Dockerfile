FROM python:3.8-slim-buster

# Install prereqs for dashboard
RUN pip install pandas dax dash pycap dash_auth

# Copy over dashboard code
COPY dashboard /opt/dashboard

# Enable external access
EXPOSE 8050

# Set our working directory
WORKDIR /opt/dashboard

# Set out our entry point to run the app
CMD ["python", "index.py"]
