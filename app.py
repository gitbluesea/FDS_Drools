# app.py
import streamlit as st
import time
import pandas as pd

# util 폴더 패키지로부터 완벽하게 격리된 기능 함수 및 클래스들만 임포트
from util.kafka_util import generate_mock_event, get_partition_key, send_alert_pipeline
from util.cep_engine import DroolsCEPEngine

st.set_page_config(page_title="FDS Real-Time Pipeline Engine", layout="wide")

st.title("🏗️ Apache Kafka & Drools Fusion 연동 실시간 FDS 파이프라인")
st.caption("모든 연산 함수가 util 폴더로 이관되었으며, 데이터 스트리밍 시작/중지 컨트롤러 버튼으로 파이프라인을 제어합니다.")

# ---------------------------------------------------------
# Streamlit 세션 내에 Stateful Drools 엔진 인스턴스 및 상태 제어 변수 보존
# ---------------------------------------------------------
if "engine" not in st.session_state:
    st.session_state.engine = DroolsCEPEngine()
if "all_events" not in st.session_state:
    st.session_state.all_events = []
if "alerts_stream" not in st.session_state:
    st.session_state.alerts_stream = []
if "is_running" not in st.session_state:
    st.session_state.is_running = False

# ==========================================
# [포인트 ③] 규칙(Rule)의 동적 변경 아키텍처 존 (사이드바)
# ==========================================
st.sidebar.header("⚙️ Dynamic Rule Configuration")
st.sidebar.info("컨슈머 중단 없이 규칙 설정을 실시간 변경하여 Drools 세션에 즉시 반영합니다.")

ui_window_sec = st.sidebar.slider("Sliding Window 탐지 범위 (초)", min_value=60, max_value=1800, value=600, step=60)
ui_threshold_cnt = st.sidebar.number_input("단시간 위험 결제 발생 임계 횟수", min_value=2, max_value=10, value=3)

# 사이드바 입력값을 util 엔진에 즉시 동적 주입
st.session_state.engine.configure_rules(window_sec=ui_window_sec, threshold_cnt=ui_threshold_cnt)

# ==========================================
# ⚡ 요구사항 반영: 시뮬레이터 제어 버튼 공간 (시작 / 중지)
# ==========================================
st.subheader("🕹️ 데이터 파이프라인 시뮬레이터 제어")
st.markdown("가상 데이터를 실시간으로 랜덤하게 대량 발생시켜 금융 사기 및 이상 징후 패턴을 테스트할 수 있습니다.")

col_btn1, col_btn2 = st.columns(2)

with col_btn1:
    if st.button("가상데이터 발생 ⚡", type="primary", width="stretch"):
        st.session_state.is_running = True

with col_btn2:
    if st.button("가상데이터 발생 중지 🛑", type="secondary", width="stretch"):
        st.session_state.is_running = False

# ==========================================
# 모니터링 대시보드 뷰어 레이아웃 자리 선점 (st.empty)
# ==========================================
col_left, col_right = st.columns(2)

with col_left:
    st.subheader("📥 실시간 수집 스트림 (Topic: payment-events)")
    payment_table_spot = st.empty()

with col_right:
    st.subheader("🚨 이상거래 실시간 대응 피드 (Topic: fds-alerts)")
    alert_feed_spot = st.empty()

st.divider()
st.subheader("📊 인메모리 Stateful Session 램 상주 현황 (Sharding 분산 키 검증)")
memory_table_spot = st.empty()

# ==========================================
# 실시간 데이터 스트리밍 연산 루프 (is_running 활성화 시 지속 가동)
# ==========================================
while st.session_state.is_running:
    # 1. 가상 데이터 랜덤 생성 유틸리티 함수 호출
    raw_payload = generate_mock_event()
    p_key = get_partition_key(raw_payload)  # [포인트 ②] 샤딩용 파티션 키 지정
    
    # 2. 인메모리 Drools CEP 엔진 연산 수행 함수 호출
    evaluation = st.session_state.engine.evaluate_event(raw_payload)
    
    # 대시보드 로그 버퍼 업데이트 (최근 10개 표시용)
    st.session_state.all_events.insert(0, raw_payload)
    st.session_state.all_events = st.session_state.all_events[:10]
    
    # 3. [핵심 3] 이상거래 탐지 조건 충족 시 알림 및 API 차단 파이프라인 트리거
    if evaluation["is_alert"]:
        alert_node = {
            "timestamp": raw_payload["eventTime"], 
            "alertMsg": evaluation["msg"], 
            "user_key": p_key
        }
        st.session_state.alerts_stream.insert(0, alert_node)
        st.session_state.alerts_stream = st.session_state.alerts_stream[:10]
        
        # fds-alerts 토픽 통신 함수 호출 연동
        send_alert_pipeline(alert_node)

    # ---- 실시간 테이블 및 컴포넌트 강제 리렌더링 ----
    with col_left:
        df_raw = pd.DataFrame(st.session_state.all_events)
        payment_table_spot.dataframe(df_raw[["userId", "amount", "country", "eventTime"]], width="stretch")

    with col_right:
        alert_contents = ""
        for alert in st.session_state.alerts_stream:
            formatted_time = time.strftime('%X', time.localtime(alert['timestamp']))
            alert_contents += f"**[{formatted_time}]** Partition Key: `{alert['user_key']}`\n\n{alert['alertMsg']}\n\n"
        if alert_contents:
            alert_feed_spot.error(alert_contents)
        else:
            alert_feed_spot.info("현재 파이프라인 상에 탐지된 이상거래 징후가 없습니다.")

    with memory_table_spot:
        session_data_list = []
        for user_key, records in st.session_state.engine.session_memory.items():
            session_data_list.append({
                "지정된 컨슈머 분산 키 (Partition Key)": user_key,
                "인메모리 상주 이벤트 카운트 (RAM)": len(records),
                "최근 Sliding Window 내 결제 국가 리스트": [r['country'] for r in records]
            })
        if session_data_list:
            st.table(pd.DataFrame(session_data_list))

    # 랜덤 데이터 인입 속도를 조절하기 위한 지연 처리 (0.5초 주기 무한 랜덤 생성)
    time.sleep(0.5)
    
    # 화면 갱신을 수행하여 스트리밍 파이프라인 상태 유지
    st.rerun()

# ==========================================
# 정지 상태일 때 고정 화면 렌더링 영역
# ==========================================
if not st.session_state.is_running:
    with col_left:
        if st.session_state.all_events:
            df_raw = pd.DataFrame(st.session_state.all_events)
            payment_table_spot.dataframe(df_raw[["userId", "amount", "country", "eventTime"]], width="stretch")
        else:
            payment_table_spot.info("발생된 데이터가 없습니다. 상단의 '가상데이터 발생 ⚡' 버튼을 눌러 시뮬레이션을 시작하세요.")

    with col_right:
        if st.session_state.alerts_stream:
            alert_contents = ""
            for alert in st.session_state.alerts_stream:
                formatted_time = time.strftime('%X', time.localtime(alert['timestamp']))
                alert_contents += f"**[{formatted_time}]** Partition Key: `{alert['user_key']}`\n\n{alert['alertMsg']}\n\n"
            alert_feed_spot.error(alert_contents)
        else:
            alert_feed_spot.info("현재 파이프라인 상에 탐지된 이상거래 징후가 없습니다.")

    with memory_table_spot:
        session_data_list = []
        for user_key, records in st.session_state.engine.session_memory.items():
            session_data_list.append({
                "지정된 컨슈머 분산 키 (Partition Key)": user_key,
                "인메모리 상주 이벤트 카운트 (RAM)": len(records),
                "최근 Sliding Window 내 결제 국가 리스트": [r['country'] for r in records]
            })
        if session_data_list:
            st.table(pd.DataFrame(session_data_list))
        else:
            st.caption("인메모리 세션에 적재된 데이터가 존재하지 않습니다.")
