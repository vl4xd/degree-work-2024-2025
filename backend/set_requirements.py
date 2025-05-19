import subprocess
import sys

# Запуск команды pip freeze и запись результата в файл
# Исправление ошибки кирилицы в пути
result = subprocess.run([sys.executable, "-m", "pip", "freeze"], capture_output=True, text=True)
with open("requirements.txt", "w", encoding="utf-8") as f:
    f.write(result.stdout)