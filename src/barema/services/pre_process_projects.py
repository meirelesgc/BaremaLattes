import os
import re
import shutil

import httpx
import pdfplumber
import polars as pl


def download_attachments(df: pl.DataFrame, input_folder: str):
    attachment_folder = os.path.join(input_folder, "attachment")
    os.makedirs(attachment_folder, exist_ok=True)

    for row in df.iter_rows(named=True):
        link = row["Link"]
        cpf = row["CPF"]

        if link == "Não encontrado":
            continue

        try:
            print(f"Baixando anexo de: {link}...")

            with httpx.stream("GET", link, timeout=15.0) as response:
                if response.status_code == 200:
                    attachment_filename = link.split("/")[-1]
                    attachment_filename = re.sub(
                        r'[\\/*?:"<>|)]', "", attachment_filename
                    )

                    if not attachment_filename:
                        attachment_filename = f"anexo_{cpf}.pdf"

                    attachment_path = os.path.join(
                        attachment_folder, attachment_filename
                    )

                    with open(attachment_path, "wb") as f:
                        for chunk in response.iter_bytes(chunk_size=8192):
                            f.write(chunk)

                    print(f"-> Anexo salvo em: {attachment_path}")
                else:
                    print(f"-> Erro ao baixar (Status {response.status_code}): {link}")

        except httpx.RequestError as req_err:
            print(f"-> Falha na conexão ao baixar {link}: {req_err}")


def extract_project_metadata(input_folder: str, output_csv: str = "resultado_cnpq.csv"):
    extracted_data = []

    name_pattern = re.compile(r"NOME:\s*(.+)", re.IGNORECASE)
    cpf_pattern = re.compile(r"CPF:\s*([\d\.\-]+)", re.IGNORECASE)
    link_pattern = re.compile(r"(http://anexosform\.cnpq\.br/doc/\S+)", re.IGNORECASE)

    print(f"Buscando PDFs na pasta: {input_folder}...")

    if not os.path.exists(input_folder):
        print(f"Erro: A pasta '{input_folder}' não existe.")
        return None

    not_processable_folder = os.path.join(input_folder, "not_processable")
    os.makedirs(not_processable_folder, exist_ok=True)

    for file_name in os.listdir(input_folder):
        if file_name.lower().endswith(".pdf"):
            file_path = os.path.join(input_folder, file_name)
            should_move = False

            try:
                with pdfplumber.open(file_path) as pdf:
                    content = ""
                    for page in pdf.pages:
                        page_text = page.extract_text()
                        if page_text:
                            content += page_text + "\n"

                    if content.strip():
                        name_match = name_pattern.search(content)
                        cpf_match = cpf_pattern.search(content)
                        link_match = link_pattern.search(content)

                        name = (
                            name_match.group(1).strip()
                            if name_match
                            else "Não encontrado"
                        )
                        cpf = (
                            cpf_match.group(1).strip()
                            if cpf_match
                            else "Não encontrado"
                        )
                        link = (
                            link_match.group(1).strip()
                            if link_match
                            else "Não encontrado"
                        )

                        if not name_match and not cpf_match:
                            should_move = True
                        else:
                            extracted_data.append(
                                {
                                    "Arquivo": file_name,
                                    "Nome": name,
                                    "CPF": cpf,
                                    "Link": link,
                                }
                            )

                            print(
                                f"Lido: {file_name} | Nome: {name} | CPF: {cpf} | Link: {link}"
                            )

                    else:
                        print(
                            f"Aviso: Não foi possível extrair texto puro de {file_name}"
                        )
                        should_move = True

            except Exception as e:
                print(f"Erro ao processar o arquivo {file_name}. Erro: {e}")

            if should_move:
                try:
                    dest_path = os.path.join(not_processable_folder, file_name)
                    shutil.move(file_path, dest_path)
                    print(f"-> Movido para not_processable: {file_name}")
                except Exception as e:
                    print(f"Erro ao mover o arquivo {file_name}: {e}")

    if extracted_data:
        df = pl.DataFrame(extracted_data)
        df.write_csv(output_csv, separator=";")
        print(f"\nFinalizado! Os dados foram salvos no arquivo '{output_csv}'.")
        return extracted_data
    else:
        print("\nNenhum dado foi extraído.")
        return None
