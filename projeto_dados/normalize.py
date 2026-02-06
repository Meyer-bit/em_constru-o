import os
import pandas as pd


class Normalizer:
    def __init__(self, input_dir, output_dir):
        self.input_dir = input_dir
        self.output_dir = output_dir
        os.makedirs(output_dir, exist_ok=True)


    def convert_columns_to_string(self, df):
        # Percorre todas as colunas do DataFrame e converte listas em strings
        for col in df.columns:
            if df[col].apply(lambda x: isinstance(x, list)).any():
                df[col] = df[col].apply(lambda x: str(x) if isinstance(x, list) else x)
        return df
    

    # Remove linhas duplicadas e reseta o índice
    def drop_duplicates(self, df):
        return df.drop_duplicates().reset_index(drop=True)


    # Monta o caminho completo do arquivo de entrada
    def load_df_from_file(self, file, ext):
        input_path = os.path.join(self.input_dir, file)
        
        if ext.lower() == ".csv":
            df = pd.read_csv(input_path)
        elif ext.lower() == ".json":
            try:
                df = pd.read_json(input_path)
            except ValueError:
                df = pd.read_json(input_path, lines=True)

        return df                



    def normalize_data(self):
        for file in os.listdir(self.input_dir):
            name, ext = os.path.splitext(file)
            
            # Define o caminho do arquivo de saída em formato parquet
            output_path = os.path.join(self.output_dir, f"{name}.parquet")


            df = self.load_df_from_file(file, ext)
            df = self.convert_columns_to_string(df)

            df = df.drop_duplicates().reset_index(drop=True)
            df.to_parquet(output_path, index=False)
            print(f"Arquivo {file} processado com sucesso e salvo em {output_path}")
            


if __name__ == "__main__":
    normalizer = Normalizer(input_dir="data/raw", output_dir="data/processed")
    normalizer.normalize_data()
