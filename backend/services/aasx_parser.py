"""AASX (Asset Administration Shell) 파일 파서.

basyx-python-sdk 로 AASX V2/V3 를 열어 MQTT 커넥터 config·태그 후보를
자동 채우기 위한 표준 dict 를 반환한다.

경량형 스코프:
- shells: [{id, id_short, asset_id}]
- submodels: [{id, id_short, semantic_id, properties: [...]}]
- technical_data / digital_nameplate: {name: value}
- 파일 원본은 이 모듈에서 저장하지 않음 (호출자가 MinIO 로 저장).
"""
from io import BytesIO

from basyx.aas import model
from basyx.aas.adapter.aasx import AASXReader


def parse_aasx(file_bytes: bytes) -> dict:
    store = model.DictObjectStore()
    with AASXReader(BytesIO(file_bytes)) as reader:
        reader.read_into(object_store=store, file_store=None)

    result = {"shells": [], "submodels": [],
              "technical_data": {}, "digital_nameplate": {}}

    for obj in store:
        if isinstance(obj, model.AssetAdministrationShell):
            result["shells"].append({
                "id": str(obj.id),
                "id_short": obj.id_short or "",
                "asset_id": _asset_id_of(obj),
            })
        elif isinstance(obj, model.Submodel):
            props = _extract_properties(obj)
            sm = {
                "id": str(obj.id),
                "id_short": obj.id_short or "",
                "semantic_id": _semantic_id_str(obj.semantic_id),
                "properties": props,
            }
            result["submodels"].append(sm)
            key = (obj.id_short or "").lower()
            if key.startswith("technicaldata"):
                result["technical_data"] = {p["id_short"]: p.get("value") for p in props}
            elif "nameplate" in key:
                result["digital_nameplate"] = {p["id_short"]: p.get("value") for p in props}

    return result


def _asset_id_of(shell):
    info = getattr(shell, "asset_information", None)
    if info is None:
        return shell.id_short or ""
    gid = getattr(info, "global_asset_id", None)
    if gid:
        return str(gid)
    return shell.id_short or ""


def _semantic_id_str(sem):
    if sem is None:
        return ""
    try:
        keys = getattr(sem, "key", None) or []
        return str(keys[0].value) if keys else str(sem)
    except Exception:
        return ""


def _extract_properties(container, prefix=""):
    out = []
    elems = getattr(container, "submodel_element", None)
    if elems is None:
        return out
    for el in elems:
        name = el.id_short or ""
        path = f"{prefix}/{name}" if prefix else name

        if isinstance(el, model.Property):
            out.append({
                "id_short": name,
                "path": path,
                "value_type": _valtype(el.value_type),
                "value": _stringify_value(el.value),
                "unit": _unit_of(el),
                "semantic_id": _semantic_id_str(el.semantic_id),
                "description": _first_lang(el.description),
            })
        elif isinstance(el, model.MultiLanguageProperty):
            out.append({
                "id_short": name,
                "path": path,
                "value_type": "string",
                "value": _first_lang(el.value),
                "unit": "",
                "semantic_id": _semantic_id_str(el.semantic_id),
                "description": _first_lang(el.description),
            })
        elif isinstance(el, model.SubmodelElementCollection):
            out.extend(_extract_properties(el, prefix=path))
    return out


def _valtype(vt):
    if vt is None:
        return "string"
    name = str(vt).lower()
    if any(k in name for k in ["float", "double", "decimal"]):
        return "float"
    if any(k in name for k in ["int", "long", "short", "byte"]):
        return "int"
    if "bool" in name:
        return "bool"
    return "string"


def _unit_of(el):
    ds_list = getattr(el, "embedded_data_specifications", None) or []
    for spec in ds_list:
        content = getattr(spec, "data_specification_content", None)
        if content is None:
            continue
        unit = getattr(content, "unit", None)
        if unit:
            return str(unit)
    return ""


def _first_lang(mlp):
    if mlp is None:
        return ""
    try:
        for _, txt in mlp.items():
            return str(txt)
    except AttributeError:
        pass
    return str(mlp) if mlp else ""


def _stringify_value(v):
    if v is None:
        return None
    if isinstance(v, (int, float, bool, str)):
        return v
    return str(v)
