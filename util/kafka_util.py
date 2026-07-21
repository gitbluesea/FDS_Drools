# util/kafka_util.py
import random
import time

def generate_mock_event() -> dict:
    """[핵심 1] 결제 및 로그인 이벤트를 수집/발행하는 가상 데이터 생성 함수"""
    # 분산 파티션 키 샤딩 테스트를 명확하게 확인하기 위해 유저 ID를 4명으로 제한
    user_id = f"user_{random.randint(1, 4)}"
    
    # 25% 확률로 해외 결제(US, VN)를 유도하여 Drools의 타국 결제 탐지 규칙 작동 유도
    country = random.choice(["KR", "KR", "KR", "US", "VN"]) 
    
    return {
        "userId": user_id,
        "amount": random.randint(5000, 800000),
        "country": country,
        "eventTime": time.time()  # 이벤트 고유 타임스탬프 (Event Time)
    }

def get_partition_key(event: dict) -> str:
    """[포인트 ②] 분산 메모리 환경에서 동일 사용자가 같은 컨슈머로 샤딩되도록 파티션 키 추출"""
    return event.get("userId", "default_key")

def send_alert_pipeline(alert_msg: dict):
    """[핵심 3] 이상거래 감지 시 fds-alerts 토픽 발행 및 차단 API 호출 연동 함수"""
    # 실제 연동 예시: kafka_producer.send('fds-alerts', value=alert_msg)
    pass
