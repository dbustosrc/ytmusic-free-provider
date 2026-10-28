FROM alpine:3.23@sha256:ff7f0c727dd796aa36787bfcd9a64bf35a6ce72a73451b3359fde849b6703d63
RUN apk add --no-cache samba-server samba-common-tools && \
    adduser -D -H -s /sbin/nologin music && \
    printf 'music-test-only\nmusic-test-only\n' | smbpasswd -s -a music
COPY smb.conf /etc/samba/smb.conf
LABEL local.music-assistant.verification="smb-fixture"
CMD ["sh", "-c", "mkdir -p /srv/music && chown music:music /srv/music && exec smbd --foreground --no-process-group --debug-stdout"]
