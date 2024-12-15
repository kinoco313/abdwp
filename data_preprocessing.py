from pathlib import Path
import ibis

"""
# やること
lahmanデータセットを作成する

# 手順
1. Lahmanディレクトリを作成
2. lahman_1871-2023_csvディレクトリにあるcsvファイルを読み込む
3. 読み込んだcsvファイルをparquetに変換する
4. parquetファイルをLahman_parquetディレクトリに格納する

# 目的
- csvファイルだと大きすぎるので, parquetファイルに変換する
- pathlibモジュールの練習
- ibisライブラリの練習
"""

lahman_parh = Path("./Lahman")
lahman_csv_path = Path("./lahman_1871-2023_csv")

def main():
    # Lahman_parquetディレクトリを作成
    try:
        lahman_parh.mkdir()
    except FileExistsError as e:
        print("Lahmanディレクトリは既に存在します", e)
    
    # lahman_1871-2023_csvディレクトリにあるcsvファイルを読み込む
    try:
        for csv_file in lahman_csv_path.iterdir():
            tmp = ibis.read_csv(csv_file.name)
            
            # 読み込んだcsvファイルをparquetに変換
            output_path = lahman_parh / csv_file.stem + ".parquet"
            
            # parquetファイルをLahman_parquetディレクトリに格納する
            tmp.to_parquet(output_path)
    except FileNotFoundError as e:
        print("ファイルまたはディレクトリが存在しません", e)
    
if __name__ == "__main__":
    main()
    

    