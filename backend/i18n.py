import json
import os
import time

_TRANSLATIONS = {}
_I18N_DIR = os.path.join(os.path.dirname(__file__), '..', 'static', 'i18n')
_DEFAULT_LANG = 'ko'
_SUPPORTED_LANGS = ('ko', 'en')
_FILE_MTIMES = {}
_LAST_CHECK = 0
_CHECK_INTERVAL = 2  # seconds


def load_translations():
    """JSON 파일에서 번역 데이터를 로드한다."""
    for lang in _SUPPORTED_LANGS:
        filepath = os.path.join(os.path.abspath(_I18N_DIR), f'{lang}.json')
        if os.path.exists(filepath):
            mtime = os.path.getmtime(filepath)
            if _FILE_MTIMES.get(lang) != mtime:
                with open(filepath, 'r', encoding='utf-8') as f:
                    _TRANSLATIONS[lang] = json.load(f)
                _FILE_MTIMES[lang] = mtime


def _ensure_fresh():
    """파일 변경 여부를 주기적으로 확인하여 자동 갱신한다."""
    global _LAST_CHECK
    now = time.time()
    if now - _LAST_CHECK >= _CHECK_INTERVAL:
        _LAST_CHECK = now
        load_translations()


def get_translation(key, lang=None):
    _ensure_fresh()
    lang = lang if lang in _SUPPORTED_LANGS else _DEFAULT_LANG
    value = _TRANSLATIONS.get(lang, {})
    for part in key.split('.'):
        if isinstance(value, dict):
            value = value.get(part)
        else:
            value = None
            break
    if value is not None and isinstance(value, str):
        return value
    if lang != _DEFAULT_LANG:
        return get_translation(key, _DEFAULT_LANG)
    return key


def get_all_translations(lang=None):
    _ensure_fresh()
    lang = lang if lang in _SUPPORTED_LANGS else _DEFAULT_LANG
    return _TRANSLATIONS.get(lang, {})


def get_current_lang():
    from flask import session, request
    lang = session.get('lang')
    if lang in _SUPPORTED_LANGS:
        return lang
    lang = request.cookies.get('lang')
    if lang in _SUPPORTED_LANGS:
        return lang
    return _get_system_default_lang()


def _get_system_default_lang():
    try:
        from backend.database import SessionLocal
        from backend.models.user import AdminSetting
        db = SessionLocal()
        row = db.query(AdminSetting).filter_by(key='system.default_language').first()
        db.close()
        if row and row.value in _SUPPORTED_LANGS:
            return row.value
    except Exception:
        pass
    return _DEFAULT_LANG
