# util/cep_engine.py
import collections

class DroolsCEPEngine:
    """Drools Fusion의 Stateful STREAM 모드 연산을 인메모리로 완전 가상화한 엔진"""
    def __init__(self):
        # 파티션 키별로 분산 저장되는 인메모리 세션 램(RAM) 구조
        self.session_memory = collections.defaultdict(list)
        # 의사 시계(PseudoClock) 상태값
        self.pseudo_clock = 0.0
        # [포인트 ③] 실시간 동적 규칙 변경 적용을 위한 설정 저장소
        self.rules_config = {
            "time_window_sec": 600,   # 10분 내 (기본값)
            "alert_threshold_cnt": 3  # 경고 발생 임계값 (기본값)
        }

    def configure_rules(self, window_sec: int, threshold_cnt: int):
        """서버 중단 없이 규칙 설정을 동적으로 갱신하는 함수"""
        self.rules_config["time_window_sec"] = window_sec
        self.rules_config["alert_threshold_cnt"] = threshold_cnt

    def evaluate_event(self, event: dict) -> dict:
        """KieSession 내에 이벤트를 insert()하고 슬라이딩 윈도우 기반 룰을 평가하는 메인 함수"""
        user_id = event.get("userId")
        event_time = event.get("eventTime", 0.0)

        # [포인트 ①] 네트워크 지연 왜곡 방지를 위해 시스템 시간이 아닌 eventTime으로 의사 시계 전진
        if event_time > self.pseudo_clock:
            self.pseudo_clock = event_time

        # 해당 유저의 과거 격리 메모리 로드 및 현재 이벤트 주입
        user_events = self.session_memory[user_id]
        user_events.append(event)

        # Sliding Window 룰: 현재 의사 시계 기준 설정 범위(예: 10분)를 지난 이벤트는 만료(Evict) 처리
        cutoff = self.pseudo_clock - self.rules_config["time_window_sec"]
        user_events = [e for e in user_events if e.get("eventTime", 0.0) >= cutoff]
        self.session_memory[user_id] = user_events

        # 탐지 규칙 1: 10분 내 서로 다른 타국 결제 패턴 분석
        countries = set(e.get("country") for e in user_events)
        
        result = {"is_alert": False, "msg": ""}

        if len(countries) >= 2:
            result = {
                "is_alert": True,
                "msg": f"🚨 [타국 결제 발견] 동일 사용자({user_id})가 {self.rules_config['time_window_sec']}초 내에 여러 국가 {list(countries)}에서 결제를 시도했습니다."
            }
        # 탐지 규칙 2: 단시간 내 임계치를 초과하는 빈도의 결제 과다 요청 분석
        elif len(user_events) >= self.rules_config["alert_threshold_cnt"]:
            result = {
                "is_alert": True,
                "msg": f"🚨 [과다 결제 감지] 동일 사용자({user_id})가 {self.rules_config['time_window_sec']}초 내 {len(user_events)}회 연속 결제를 시도했습니다."
            }

        return result
