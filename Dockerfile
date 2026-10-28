# Base image version (moving tags: latest, beta, nightly)
# Declared before the first FROM so it is usable in the FROM line below.
ARG MA_VERSION=latest
ARG BASE_IMAGE=ghcr.io/music-assistant/server:${MA_VERSION}

# --- Stage: runtime -----------------------------------------------------------
FROM ${BASE_IMAGE} AS runtime

# Add OCI labels for basic image introspection
LABEL org.opencontainers.image.source="https://github.com/dbustosrc/ytmusic-free-provider" \
      org.opencontainers.image.description="Unofficial build of the upstream server image with the ytmusic_free YouTube Music provider pre-installed. Independent community project, not affiliated with or endorsed by the upstream project."

# Copy the provider directory from the build context into the base image and
# detect the active Python version to move files to the correct site-packages
# folder.
COPY ytmusic_free/ /tmp/ytmusic_free
# Resolve the site-packages path via Python itself (no globbing, no
# hard-coded version fallback): if the venv Python is broken, the build
# fails here instead of silently installing to the wrong location.
RUN DST_DIR="$( /app/venv/bin/python -c 'import sysconfig; print(sysconfig.get_path("purelib"))' )/music_assistant/providers/ytmusic_free" && \
    rm -rf "$DST_DIR" && \
    mv /tmp/ytmusic_free "$DST_DIR"
RUN /app/venv/bin/uv pip install --python /app/venv/bin/python --no-cache 'smbprotocol==1.17.0'
