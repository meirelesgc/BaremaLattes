import glob
import json
import os
from pprint import pprint

import xmltodict


def _canonical_key(s):
    if isinstance(s, str):
        return f"str:{s}"
    if isinstance(s, dict):
        items = sorted((k, _canonical_key(v)) for k, v in s.items())
        return "dict:{" + ",".join(f"{k}:{v}" for k, v in items) + "}"
    if isinstance(s, list):
        return "list:[" + ",".join(_canonical_key(x) for x in s) + "]"
    return f"other:{repr(s)}"


def merge_schemas(a, b):
    if a == b:
        return a

    if isinstance(a, dict) and isinstance(b, dict):
        keys = set(a) | set(b)
        merged = {}
        for k in sorted(keys):
            if k in a and k in b:
                merged[k] = merge_schemas(a[k], b[k])
            elif k in a:
                merged[k] = a[k]
            else:
                merged[k] = b[k]
        return merged

    if isinstance(a, list) and isinstance(b, list):
        if len(a) == 1 and len(b) == 1:
            merged = merge_schemas(a[0], b[0])
            return [merged]
        flat = []
        seen = set()
        for item in a + b:
            key = _canonical_key(item)
            if key not in seen:
                seen.add(key)
                flat.append(item)
        return flat

    if isinstance(a, list):
        return merge_schemas(a, [b])
    if isinstance(b, list):
        return merge_schemas([a], b)

    return [a, b]


def infer_schema(value):
    if value is None:
        return "NoneType"
    if isinstance(value, bool):
        return "bool"
    if isinstance(value, int) and not isinstance(value, bool):
        return "int"
    if isinstance(value, float):
        return "float"
    if isinstance(value, str):
        return "str"
    if isinstance(value, dict):
        schema = {}
        for k, v in value.items():
            schema[k] = infer_schema(v)
        return schema
    if isinstance(value, list):
        if len(value) == 0:
            return ["?"]
        elem_schema = infer_schema(value[0])
        for elem in value[1:]:
            elem_schema = merge_schemas(elem_schema, infer_schema(elem))
        return [elem_schema]
    return type(value).__name__


def infer_schema_from_list(data_list):
    global_schema = {}
    for item in data_list:
        item_schema = infer_schema(item)
        if not global_schema:
            global_schema = item_schema
            continue
        global_schema = merge_schemas(global_schema, item_schema)
    return global_schema


def normalize_xml_lists(obj):
    if isinstance(obj, dict):
        for k, v in list(obj.items()):
            obj[k] = normalize_xml_lists(v)
        return obj
    if isinstance(obj, list):
        return [normalize_xml_lists(x) for x in obj]
    return obj


def ensure_lists(obj):
    if isinstance(obj, dict):
        for k, v in list(obj.items()):
            # Se o valor for uma lista, aplica recursivamente
            if isinstance(v, list):
                obj[k] = [ensure_lists(x) for x in v]
            elif isinstance(v, dict):
                if any(c.isupper() for c in k) and "-" in k:
                    obj[k] = [ensure_lists(v)]
                else:
                    obj[k] = ensure_lists(v)
            else:
                obj[k] = v
        return obj
    elif isinstance(obj, list):
        return [ensure_lists(x) for x in obj]
    return obj


def load_xml_files_from_dir(path="curriculos", pattern="*.xml"):
    LIMIT = None
    files = sorted(glob.glob(os.path.join(path, pattern)))

    if LIMIT is not None:
        files = files[:LIMIT]

    docs = []
    print(f"Carregando {len(files)} arquivo(s)...")

    for f in files:
        try:
            with open(f, "r", encoding="ISO-8859-1") as fh:
                xml_data = fh.read()
                doc = xmltodict.parse(xml_data)
                docs.append(doc)
        except Exception as e:
            print(f"Erro ao ler {f}: {e}")

    return docs


def normalize_and_merge_schema_lists(schema_node):
    if isinstance(schema_node, dict):
        return {k: normalize_and_merge_schema_lists(v) for k, v in schema_node.items()}

    if isinstance(schema_node, list):
        items_to_process = schema_node

        if len(items_to_process) == 1 and isinstance(items_to_process[0], list):
            items_to_process = items_to_process[0]

        if items_to_process and all(
            isinstance(item, dict) for item in items_to_process
        ):
            merged_dict = merge_list_of_dicts(items_to_process)
            return [normalize_and_merge_schema_lists(merged_dict)]
        else:
            return [normalize_and_merge_schema_lists(item) for item in items_to_process]

    return schema_node


def merge_list_of_dicts(list_of_dicts):
    merged = {}
    for d in list_of_dicts:
        merged = merge_schemas(merged, d)
    return merged


def merge_similar_dicts_in_list(lst):
    if not all(isinstance(x, dict) for x in lst):
        return lst

    merged = {}
    for d in lst:
        merged = merge_schemas(merged, d)
    return [merged]


def compact_schema_lists(schema):
    if isinstance(schema, dict):
        return {k: compact_schema_lists(v) for k, v in schema.items()}
    if isinstance(schema, list):
        processed = [compact_schema_lists(x) for x in schema]
        return merge_similar_dicts_in_list(processed)
    return schema


if __name__ == "__main__":
    docs = load_xml_files_from_dir("storage/curriculos")
    raw_schema = infer_schema_from_list(docs)
    schema = normalize_and_merge_schema_lists(raw_schema)
    schema = compact_schema_lists(schema)

    print("Schema inferido e normalizado:")
    output_filename = "schema.json"
    with open(output_filename, "w", encoding="utf-8") as f:
        json.dump(schema, f, indent=4, ensure_ascii=False)

    print(f"\nSchema salvo com sucesso em: '{output_filename}'")

    if False:
        ...

    other_production_keys = schema["CURRICULO-VITAE"].keys()
    print("\nChaves unificadas:")
    pprint(other_production_keys)
    print(f"\nTotal de tipos unificados: {len(other_production_keys)}")

    PRODUCAO_BIBLIOGRAFICA = set()
    for itens in schema["CURRICULO-VITAE"]["PRODUCAO-BIBLIOGRAFICA"]:
        if isinstance(itens, str):
            continue
        if itens.get("DEMAIS-TIPOS-DE-PRODUCAO-BIBLIOGRAFICA"):
            PRODUCAO_BIBLIOGRAFICA.update(
                itens["DEMAIS-TIPOS-DE-PRODUCAO-BIBLIOGRAFICA"].keys()
            )
        PRODUCAO_BIBLIOGRAFICA.update(itens.keys())
    pprint(PRODUCAO_BIBLIOGRAFICA)

    PRODUCAO_TECNICA = set()
    for itens in schema["CURRICULO-VITAE"]["PRODUCAO-TECNICA"]:
        if isinstance(itens, str):
            continue
        if itens.get("DEMAIS-TIPOS-DE-PRODUCAO-TECNICA"):
            PRODUCAO_TECNICA.update(itens["DEMAIS-TIPOS-DE-PRODUCAO-TECNICA"].keys())
        PRODUCAO_TECNICA.update(itens.keys())
    pprint(PRODUCAO_TECNICA)

    PRODUCAO_ARTISTICA_CULTURAL = set()
    for itens in schema["CURRICULO-VITAE"]["OUTRA-PRODUCAO"]:
        if isinstance(itens, str):
            continue
        if itens.get("PRODUCAO-ARTISTICA-CULTURAL"):
            PRODUCAO_ARTISTICA_CULTURAL.update(
                itens["PRODUCAO-ARTISTICA-CULTURAL"].keys()
            )
    pprint(PRODUCAO_ARTISTICA_CULTURAL)
