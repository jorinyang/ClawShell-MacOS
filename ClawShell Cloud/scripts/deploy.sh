#!/usr/bin/env bash
set -euo pipefail

# ============================================================
# ClawShell Cloud — 阿里云 ECS 一键部署脚本
# 用法: ./deploy.sh <ecs_ip> <ssh_key_path>
# ============================================================

RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'; NC='\033[0m'

ECS_IP=${1:-}
SSH_KEY=${2:-~/.ssh/id_rsa}
DEPLOY_USER=root

if [[ -z "$ECS_IP" ]]; then
    echo -e "${RED}用法: $0 <ecs_ip> <ssh_key_path>${NC}"
    echo -e "示例: $0 47.92.168.1 ~/.ssh/ecs_key"
    exit 1
fi

echo -e "${GREEN}[1/6] 检查 SSH 连接...${NC}"
if ! ssh -i "$SSH_KEY" -o ConnectTimeout=5 ${DEPLOY_USER}@${ECS_IP} "echo ok" > /dev/null 2>&1; then
    echo -e "${RED}SSH 连接失败，请检查 IP 和密钥${NC}"
    exit 1
fi
echo -e "${GREEN}SSH 连接成功${NC}"

echo -e "${GREEN}[2/6] 安装 Docker 和 Docker Compose...${NC}"
ssh -i "$SSH_KEY" ${DEPLOY_USER}@${ECS_IP} << 'SSHEOF'
if ! command -v docker &>/dev/null; then
    curl -fsSL https://get.docker.com | sh
    systemctl enable docker
    systemctl start docker
fi
if ! command -v docker compose &>/dev/null; then
    apt-get install -y docker-compose-plugin
fi
echo "Docker version: $(docker --version)"
SSHEOF

echo -e "${GREEN}[3/6] 创建部署目录...${NC}"
ssh -i "$SSH_KEY" ${DEPLOY_USER}@${ECS_IP} "mkdir -p /opt/clawshell && ls /opt/"
echo -e "${GREEN}[4/6] 上传部署文件...${NC}"
rsync -avz --delete -e "ssh -i $SSH_KEY" \
    "$(dirname "$0")/../" \
    ${DEPLOY_USER}@${ECS_IP}:/opt/clawshell/

echo -e "${YELLOW}[5/6] 配置环境变量...${NC}"
read -rp "请输入 JWT_SECRET (用于认证): " JWT_SECRET
read -rp "请输入 USER_ID (你的用户标识): " USER_ID

ssh -i "$SSH_KEY" ${DEPLOY_USER}@${ECS_IP} << 'SSHEOF'
cd /opt/clawshell
export JWT_SECRET="$JWT_SECRET"
export USER_ID="$USER_ID"

# 配置 firewall 安全组 (可选)
# aliyun ecs AuthorizeSecurityGroup --RegionId cn-hangzhou ...

echo -e "${GREEN}[6/6] 启动服务...${NC}"
docker compose up -d --build

echo "等待服务启动..."
sleep 10
docker compose ps
echo ""
echo -e "${GREEN}======================================"
echo -e "  部署完成！"
echo -e "  Cloud Hub: https://${ECS_IP}:443"
echo -e "  WebSocket: wss://${ECS_IP}:443/hub"
echo -e "======================================${NC}"
SSHEOF

echo -e "${GREEN}部署脚本执行完毕${NC}"
