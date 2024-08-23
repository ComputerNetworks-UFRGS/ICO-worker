import pandas as pd
import pyarrow.parquet as pq
import os


def read_parquet(file_name: str) -> pd.DataFrame:
    """
    Lê um arquivo Parquet e retorna os dados como um DataFrame do pandas.

    :param file_path: Caminho para o arquivo Parquet.
    :return: DataFrame contendo os dados do arquivo Parquet.
    """

    base_dir = os.path.dirname(os.path.abspath(__file__))
    # Constrói o caminho completo para o arquivo Parquet
    file_path = os.path.join(base_dir, file_name)

    # Verifica se o arquivo existe
    if not os.path.isfile(file_path):
        raise FileNotFoundError(f"Arquivo não encontrado: {file_path}")

    table = pq.read_table(file_path)
    df = table.to_pandas()
    return df


# Exemplo de uso
if __name__ == "__main__":
    file_path = "version_1_handle_3_2024-07-26 00:38:49.004215.parquet"
    df = read_parquet(file_path)
    print(df)
