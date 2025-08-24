import csv

input_file = "signals_log.csv"
output_file = "signals_log_fixed.csv"

with open(input_file, "r", newline='') as f_in, open(output_file, "w", newline='') as f_out:
    reader = csv.reader(f_in)
    writer = csv.writer(f_out)
    for row in reader:
        # Если строка слишком короткая (нет result)
        if len(row) == 10:  # до target_price
            # Вставляем '-1' после volume (индекс 7)
            row = row[:8] + ["-1"] + row[8:]
        writer.writerow(row)

print(f"Готово! Исправленный файл: {output_file}")