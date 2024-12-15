from pathlib import Path
import duckdb

"""
# やること
lahmanデータセットを作成する

# 手順
1. Lahmanディレクトリを作成
2. lahman_1871-2023_csvディレクトリにあるcsvファイルを読み込む
3. 読み込んだcsvファイルをparquetに変換する
4. parquetファイルをLahman_parquetディレクトリに格納する

# 目的
- csvファイルだと大きくて遅いので, parquetファイルに変換する
- pathlibの練習
- duckdbの練習
"""

lahman_path = Path("./Lahman")
lahman_csv_path = Path("./lahman_1871-2023_csv")


def main():
    # Lahman_parquetディレクトリを作成
    try:
        lahman_path.mkdir()
    except FileExistsError as e:
        print("Lahmanディレクトリは既に存在します", e)

    # lahman_1871-2023_csvディレクトリにあるcsvファイルを読み込む
    try:
        for csv_file in lahman_csv_path.iterdir():
            tmp = duckdb.read_csv(csv_file)
            # 読み込んだcsvファイルをparquetに変換
            parquet_path = Path(csv_file.stem + ".parquet")
            output_path = lahman_path / parquet_path

            # parquetファイルをLahman_parquetディレクトリに格納する
            tmp.write_parquet(str(output_path))
    except FileNotFoundError as e:
        print("ファイルまたはディレクトリが存在しません", e)


if __name__ == "__main__":
    main()
