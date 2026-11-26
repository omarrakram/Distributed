# One image for every Python component (api, broker, agent, autoscaler, migrate, worker).
# The component is selected by the command: python -m dtq <component>
FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

WORKDIR /app

# Dependencies first (cached layer); versions are fully pinned in requirements.txt.
# Behind a TLS-inspecting corporate proxy, pass its CA as an optional BuildKit secret:
#   docker build --secret id=ca,src=/path/to/proxy-ca.crt ...   (unused otherwise)
COPY requirements.txt ./
RUN --mount=type=secret,id=ca \
    if [ -s /run/secrets/ca ]; then export PIP_CERT=/run/secrets/ca; fi; \
    pip install -r requirements.txt

COPY migrations ./migrations
COPY dtq ./dtq

# Run as an unprivileged user by default (the agent service overrides this in compose
# because it must talk to the Docker socket).
RUN useradd --system --uid 10001 --home-dir /app --shell /usr/sbin/nologin dtq \
    && chown -R dtq:dtq /app
USER dtq

EXPOSE 8000 8001 8003
CMD ["python", "-m", "dtq", "api"]
