# from .interfaces import DatabaseInterface, StorageInterface
from io import BytesIO
from database import create_database_interface
from storage import create_storage_interface
import xml.etree.cElementTree as ET
import hashlib, traceback
from datetime import datetime


def hash_text(text):
    """
    Receives a text and returns its SHA-256 hash of a text content
    """

    # Cria um objeto sha256
    hasher = hashlib.sha256()

    # Atualiza o objeto com o texto codificado em UTF-8
    hasher.update(text.encode('utf-8'))

    # Obtém o hash hexadecimal
    return hasher.hexdigest()    

def create_xml_for_territory_and_year(territory_info:tuple, database, storage):
    """
    Create a .xml files for each year of gazettes for a territory  
    """

    actual_year = datetime.now().year
    base_year = 1960

    for year in range(base_year, actual_year+1):
        query_content = list(database.select(f"SELECT * FROM gazettes\
                                        WHERE territory_id='{territory_info[0]}' AND\
                                        date BETWEEN '{year}-01-01' AND '{year}-12-31'\
                                        ORDER BY date ASC;"))

        if len(query_content) > 0:
            root = ET.Element("root")
            meta_info_tag = ET.SubElement(root, "meta")
            ET.SubElement(meta_info_tag, "localidade", name="municipio").text = territory_info[1]
            ET.SubElement(meta_info_tag, "localidade", name="estado").text = territory_info[2]
            ET.SubElement(meta_info_tag, "criado_em").text = str(datetime.now())
            ET.SubElement(meta_info_tag, "ano").text = str(year)
            all_gazettes_tag = ET.SubElement(root, "diarios")  

            path_xml = f"{territory_info[0]}/{year}/{territory_info[1]} - {territory_info[2]} - {year}.xml"

            for gazette in query_content:
                try:
                    file_gazette_txt = BytesIO()
                    path_arq_bucket = str(gazette[7]).replace(".pdf",".txt") # É a posição 7 que contem o caminho do arquivo dentro do S3
                    
                    storage.get_file(path_arq_bucket, file_gazette_txt)

                except:
                    print(f"Erro na obtenção do conteúdo de texto do diário de {territory_info[1]} - {territory_info[2]} - {gazette[2]}")
                    continue

                gazette_tag = ET.SubElement(all_gazettes_tag, "gazette")
                meta_gazette = ET.SubElement(gazette_tag, "meta")
                ET.SubElement(meta_gazette, "URL_PDF").text = gazette[8]
                ET.SubElement(meta_gazette, "poder").text = gazette[5]
                ET.SubElement(meta_gazette, "ddicao_Extra").text = 'Sim' if gazette[4] else 'Não'
                ET.SubElement(meta_gazette, "numero_Edicao").text = str(gazette[3]) if str(gazette[3]) is not None else "Não há"
                ET.SubElement(meta_gazette, "data_diario").text = datetime.strftime(gazette[2], "%d/%m")
                ET.SubElement(gazette_tag, "conteudo").text = file_gazette_txt.getvalue().decode('utf-8')

                file_gazette_txt.close()
            
            tree = ET.ElementTree(root)

            file_xml = BytesIO()

            tree.write(file_xml, encoding='utf-8', xml_declaration=True)
            file_xml.seek(0) # Volta o cursor de leitura do arquivo para o começo dele

            content_file_xml = file_xml.getvalue().decode('utf-8')

            storage.upload_content(path_xml, content_file_xml)

            file_xml.close()
        else:
            "Teste de saida"
            # print(f"Nada encontrado para cidade {territory_info[1]}-{territory_info[2]} no ano {year}")

def create_xml_territories():

    database = create_database_interface()
    storage = create_storage_interface()

    print("Script que agrega os arquivos .txt para .xml")

    # results_query = database.select("SELECT * FROM territories WHERE name='Sampaio' OR name='Xique-Xique';")
    results_query = database.select("SELECT * FROM territories;")

    for t in results_query:
        try:
            create_xml_for_territory_and_year(t, database, storage)
        except:
            print(traceback.format_exc())
            continue


if __name__ == "__main__":
    create_xml_territories()