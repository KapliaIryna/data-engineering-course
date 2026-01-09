## Простий статичний вебсервер на базі nginx, що віддає сторінку Hello, World на порту 8080.

### Структура:

- Dockerfile
- index.html
- default.conf

### Збірка образу:

`docker build -t website-img .`

### Запуск контейнера:

---

_через Docker Hub:_

`docker run -d -p 8080:8080 –name irynakaplia-container irynakaplia/website-img:1.0`

_локальна збірка:_

1. Збірка образу з поточного каталогу:
   docker build -t website-img .
2. Запуск контейнера:
   docker run -d -p 8080:8080 –name website-container website-img

- якщо всередині nginx слухає порт 80 (стандартний конфіг):
  docker run -d -p 8080:80 –name website-container website-img

**Перевірка:**
http://localhost:8080
