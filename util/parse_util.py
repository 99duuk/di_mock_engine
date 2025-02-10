import json


def safe_json_deserializer(m):
    """JSON 메시지를 안전하게 디코딩합니다."""
    if m is None:
        return None
    try:
        return json.loads(m.decode('utf-8'))
    except (json.JSONDecodeError, AttributeError, UnicodeDecodeError) as e:
        print(f"[Deserialization Error] Failed to deserialize message: {m}, Error: {e}")
        return None