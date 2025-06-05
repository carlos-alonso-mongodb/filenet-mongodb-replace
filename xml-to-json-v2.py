import xml.etree.ElementTree as ET
import json

EXCLUDED_TAGS = {"AES"}
EXCLUDED_ATTRIBUTES = {"{http://www.w3.org/2001/XMLSchema-instance}schemaLocation"}

def convert_element(element):
    tag = element.tag.split("}", 1)[-1]
    if tag in EXCLUDED_TAGS:
        return None

    # Filtrar atributos
    attrib = {
        k: v for k, v in element.attrib.items() if k not in EXCLUDED_ATTRIBUTES
    }

    result = {}
    if attrib:
        result.update(attrib)

    children = list(element)
    if children:
        grouped = {}
        for child in children:
            child_tag = child.tag.split("}", 1)[-1]
            if child_tag in EXCLUDED_TAGS:
                continue
            value = convert_element(child)
            if value is None:
                continue
            if child_tag in grouped:
                if isinstance(grouped[child_tag], list):
                    grouped[child_tag].append(value)
                else:
                    grouped[child_tag] = [grouped[child_tag], value]
            else:
                grouped[child_tag] = value
        result.update(grouped)
    else:
        text = element.text.strip() if element.text else None
        if result:
            result['valor'] = text
        else:
            return text

    return result

def xml_to_custom_json(xml_string):
    root = ET.fromstring(xml_string)
    return convert_element(root)

def main():
    with open("entrada.xml", "r", encoding="utf-8") as f:
        xml_string = f.read()

    json_data = xml_to_custom_json(xml_string)

    with open("salida.json", "w", encoding="utf-8") as f:
        json.dump(json_data, f, indent=2, ensure_ascii=False)

    print("Conversión completada. Archivo 'salida.json' generado sin 'AES' ni 'schemaLocation'.")

if __name__ == "__main__":
    main()
