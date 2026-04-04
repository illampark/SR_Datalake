#!/bin/bash
# ============================================================
# 02_install_docker.sh — 대상 서버에서 실행 (Docker 미설치 시)
# 주의: 이 스크립트는 인터넷이 필요합니다.
#       폐쇄망에서는 Docker 패키지를 별도로 준비하세요.
# 실행: sudo bash 02_install_docker.sh
# ============================================================
set -e

echo "======================================"
echo "  Docker Engine 설치"
echo "======================================"

# 기존 설치 확인
if command -v docker &>/dev/null; then
    echo "Docker가 이미 설치되어 있습니다: $(docker --version)"
    if command -v docker compose &>/dev/null; then
        echo "Docker Compose 플러그인 확인: $(docker compose version)"
        echo "추가 설치가 필요하지 않습니다."
        exit 0
    fi
fi

# OS 감지
if [ -f /etc/os-release ]; then
    . /etc/os-release
    OS_ID="$ID"
else
    echo "지원하지 않는 OS입니다."
    exit 1
fi

case "$OS_ID" in
    ubuntu|debian)
        echo "=== Ubuntu/Debian 환경 — Docker 설치 ==="
        apt-get update
        apt-get install -y ca-certificates curl gnupg
        install -m 0755 -d /etc/apt/keyrings
        curl -fsSL https://download.docker.com/linux/$OS_ID/gpg \
            | gpg --dearmor -o /etc/apt/keyrings/docker.gpg
        chmod a+r /etc/apt/keyrings/docker.gpg

        echo \
          "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.gpg] \
          https://download.docker.com/linux/$OS_ID \
          $(. /etc/os-release && echo "$VERSION_CODENAME") stable" \
          | tee /etc/apt/sources.list.d/docker.list > /dev/null

        apt-get update
        apt-get install -y docker-ce docker-ce-cli containerd.io \
                           docker-buildx-plugin docker-compose-plugin
        ;;
    rhel|centos|rocky|almalinux)
        echo "=== RHEL 계열 — Docker 설치 ==="
        dnf install -y dnf-plugins-core
        dnf config-manager --add-repo https://download.docker.com/linux/centos/docker-ce.repo
        dnf install -y docker-ce docker-ce-cli containerd.io \
                       docker-buildx-plugin docker-compose-plugin
        ;;
    *)
        echo "지원하지 않는 OS: $OS_ID"
        echo "Docker를 수동으로 설치해주세요."
        exit 1
        ;;
esac

# Docker 서비스 시작
systemctl enable --now docker
echo ""
echo "=== Docker 설치 완료 ==="
docker --version
docker compose version

# 현재 사용자를 docker 그룹에 추가 (재로그인 필요)
SUDO_USER_NAME="${SUDO_USER:-$USER}"
if [ "$SUDO_USER_NAME" != "root" ]; then
    usermod -aG docker "$SUDO_USER_NAME"
    echo ""
    echo "  '$SUDO_USER_NAME' 사용자를 docker 그룹에 추가했습니다."
    echo "  적용하려면 재로그인하세요."
fi
