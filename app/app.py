from flask import Flask, render_template, request, session, redirect, url_for, flash, send_file
import psycopg2
from psycopg2.extras import RealDictCursor
import math
import os
import pandas as pd
import tempfile
from reportlab.lib import colors
from reportlab.lib.pagesizes import A4
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.ttfonts import TTFont
from reportlab.lib.enums import TA_CENTER
from reportlab.lib.units import inch
import io
from minio import Minio
from minio.error import S3Error
import hashlib
import uuid
from datetime import datetime
from zoneinfo import ZoneInfo
from sqlalchemy import create_engine, text
import xlsxwriter
import bcrypt

# Конфигурация MinIO
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "localhost:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
MINIO_BUCKET_NAME = "farmers-markets"

minio_client = None

def get_minio_client():
    global minio_client
    if minio_client is None:
        minio_client = Minio(
            os.getenv("MINIO_ENDPOINT", "localhost:9000"),
            access_key=os.getenv("MINIO_ACCESS_KEY", "minioadmin"),
            secret_key=os.getenv("MINIO_SECRET_KEY", "minioadmin"),
            secure=False
        )
        try:
            if not minio_client.bucket_exists(MINIO_BUCKET_NAME):
                minio_client.make_bucket(MINIO_BUCKET_NAME)
        except S3Error as e:
            if e.code != "BucketAlreadyOwnedByYou":
                raise
    return minio_client

app = Flask(__name__)
app.secret_key = os.getenv("FLASK_SECRET_KEY", "k3Vz8fGq2XpL9mNwR4sT7yUoI1aB5cD6eF0hJ2nM4qP7rS9tW")

# Конфигурация подключения к БД
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_NAME = os.getenv("DB_NAME", "ru_farmers")
DB_USER = os.getenv("DB_USER", "postgres")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_PASSWORD = os.getenv("DB_PASSWORD", "135Qr680!")

def get_db_connection():
    try:
        return psycopg2.connect(
            host=DB_HOST,
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
            cursor_factory=RealDictCursor
        )
    except Exception as e:
        print(f"Ошибка подключения к БД: {e}")
        return None

def save_file_to_minio_and_log(file_path, original_filename, operation_type, user_ip):
    """
    Сохраняет файл в MinIO и записывает метаданные в БД.
    Возвращает hashed_filename.
    """
    # Определяем расширение
    ext = os.path.splitext(original_filename)[1].lower()
    if not ext:
        raise ValueError("Файл должен иметь расширение")

    # Генерируем хеш: UUID + timestamp + IP → SHA256
    hash_input = f"{uuid.uuid4()}-{datetime.utcnow().isoformat()}-{user_ip}"
    hashed_name = hashlib.sha256(hash_input.encode()).hexdigest() + ext

    # Загружаем в MinIO
    try:
        client = get_minio_client()
        client.fput_object(MINIO_BUCKET_NAME, hashed_name, file_path)
    except S3Error as e:
        raise Exception(f"Ошибка MinIO: {e}")

    # Логируем в БД
    conn = get_db_connection()
    if not conn:
        raise Exception("Нет подключения к БД для логирования")

    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO file_logs (
                    original_filename, hashed_filename, operation_type,
                    file_extension, user_ip
                ) VALUES (%s, %s, %s, %s, %s)
            """, (original_filename, hashed_name, operation_type, ext, user_ip))
            conn.commit()
    finally:
        conn.close()

    return hashed_name

def haversine(lat1, lon1, lat2, lon2):
    R = 3958.8
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = (math.sin(dlat / 2) ** 2 +
         math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) * math.sin(dlon / 2) ** 2)
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    return R * c

@app.route('/', methods=['GET', 'POST'])
def login():
    if request.method == 'POST':
        username = request.form.get('username', '').strip()
        password = request.form.get('password', '').strip()

        if not username or not password:
            flash("Введите логин и пароль", "error")
            return render_template('login.html')

        conn = get_db_connection()
        if not conn:
            flash("Ошибка подключения к БД", "error")
            return render_template('login.html')

        try:
            with conn.cursor() as cur:
                cur.execute("SELECT id, username, password_hash FROM users WHERE username = %s", (username,))
                user = cur.fetchone()

                if user and bcrypt.checkpw(password.encode('utf-8'), user['password_hash'].encode('utf-8')):
                    session['authenticated'] = True
                    session['user_id'] = user['id']
                    session['username'] = user['username']
                    return redirect(url_for('markets'))
                else:
                    flash("Неверный логин или пароль", "error")
        finally:
            conn.close()

    return render_template('login.html')

def require_auth(f):
    def wrapper(*args, **kwargs):
        if not session.get('authenticated'):
            return redirect(url_for('login'))
        return f(*args, **kwargs)
    wrapper.__name__ = f.__name__
    return wrapper

@app.route('/markets')
@require_auth
def markets():
    try:
        page = int(request.args.get('page', 1))
        if page < 1:
            page = 1
    except (TypeError, ValueError):
        page = 1

    per_page = 10
    offset = (page - 1) * per_page

    conn = get_db_connection()
    if not conn:
        flash("Ошибка подключения к БД", "error")
        return redirect(url_for('login'))

    try:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) AS total FROM farmers_markets")
            total = cur.fetchone()['total']
            total_pages = (total + per_page - 1) // per_page

            if page > total_pages and total_pages > 0:
                return redirect(url_for('markets', page=total_pages))

            cur.execute("""
                SELECT fm.market_name, fm.city, fm.state,
                       COALESCE(AVG(r.rating), 0) AS avg_rating,
                       COUNT(r.review_id) AS review_count
                FROM farmers_markets fm
                LEFT JOIN reviews r ON fm.market_id = r.market_id
                GROUP BY fm.market_id, fm.market_name, fm.city, fm.state
                ORDER BY fm.market_name
                LIMIT %s OFFSET %s
            """, (per_page, offset))

            paginated = cur.fetchall()

            markets = []
            for m in paginated:
                rating = round(m['avg_rating'], 1)
                stars = "★" * int(round(rating)) + "☆" * (5 - int(round(rating)))
                markets.append({
                    "name": m['market_name'],
                    "city": m['city'],
                    "state": m['state'],
                    "rating": rating,
                    "stars_display": f"{stars} ({rating})",
                    "reviews": m['review_count']
                })

            return render_template('markets.html',
                                   markets=markets,
                                   current_page=page,
                                   total_pages=total_pages)
    finally:
        conn.close()

@app.route('/search', methods=['GET'])
@require_auth
def search_page():
    mode = request.args.get('mode', 'city')
    radius = request.args.get('radius') == '1'
    sort = request.args.get('sort', '0')
    q = request.args.get('q', '').strip()
    lat = request.args.get('lat')
    lon = request.args.get('lon')
    radius_val = request.args.get('radius_val')

    results = []
    if request.args:
        conn = get_db_connection()
        if conn:
            try:
                with conn.cursor() as cur:
                    if radius:
                        try:
                            lat_f = float(lat)
                            lon_f = float(lon)
                            radius_f = float(radius_val)
                        except (ValueError, TypeError):
                            flash("Некорректные координаты или радиус", "error")
                            return render_template('search.html', mode=mode, radius=radius, sort=sort, lat=lat, lon=lon, radius_val=radius_val)

                        cur.execute("SELECT market_name, city, state, y AS lat, x AS lon FROM farmers_markets")
                        for row in cur.fetchall():
                            if row['lat'] is None or row['lon'] is None:
                                continue
                            dist = haversine(lat_f, lon_f, row['lat'], row['lon'])
                            if dist <= radius_f:
                                results.append({
                                    "name": row['market_name'],
                                    "city": row['city'],
                                    "state": row['state'],
                                    "distance": round(dist, 1)
                                })
                    else:
                        if not radius and not q:
                            flash("Для поиска по городу/субъекту/индексу введите значение", "error")
                            return render_template('search.html', mode=mode, radius=radius, sort=sort, q=q, lat=lat,
                                                   lon=lon, radius_val=radius_val)

                        elif radius:
                            try:
                                lat_f = float(lat)
                                lon_f = float(lon)
                                radius_f = float(radius_val)
                            except (ValueError, TypeError):
                                flash("Некорректные координаты или радиус", "error")
                                return render_template('search.html', mode=mode, radius=radius, sort=sort, lat=lat,
                                                       lon=lon, radius_val=radius_val)

                        cur.execute(f"""
                            SELECT market_name, city, state
                            FROM farmers_markets
                            WHERE LOWER(TRIM({mode})) = %s
                        """, (q.lower(),))
                        results = [{"name": r['market_name'], "city": r['city'], "state": r['state']} for r in cur.fetchall()]

                    # Сортировка по рейтингу
                    if sort == "3" and results:
                        market_names = [m["name"] for m in results]
                        placeholders = ','.join(['%s'] * len(market_names))
                        cur.execute(f"""
                            SELECT fm.market_name, COALESCE(AVG(r.rating), 0) AS avg_rating
                            FROM farmers_markets fm
                            LEFT JOIN reviews r ON fm.market_id = r.market_id
                            WHERE fm.market_name IN ({placeholders})
                            GROUP BY fm.market_name
                        """, market_names)
                        rating_map = {r['market_name']: float(r['avg_rating']) for r in cur.fetchall()}
                        for m in results:
                            m['rating'] = rating_map.get(m['name'], 0.0)
                        results.sort(key=lambda x: x.get('rating', 0), reverse=True)
                    elif sort == "1":
                        results.sort(key=lambda x: x["name"])
                    elif sort == "2":
                        results.sort(key=lambda x: x["name"], reverse=True)

            except Exception as e:
                flash(f"Ошибка поиска: {e}", "error")
            finally:
                conn.close()

    return render_template('search.html',
                         mode=mode,
                         radius=radius,
                         sort=sort,
                         q=q,
                         lat=lat,
                         lon=lon,
                         radius_val=radius_val,
                         results=results)

@app.route('/detail', methods=['GET'])
@require_auth
def detail_page():
    name = request.args.get('name', '').strip()
    market = None
    if name:
        conn = get_db_connection()
        if conn:
            try:
                with conn.cursor() as cur:
                    cur.execute("SELECT * FROM farmers_markets WHERE LOWER(TRIM(market_name)) = %s", (name.lower(),))
                    row = cur.fetchone()
                    if row:
                        # Продукты
                        cur.execute("""
                            SELECT p.product_name
                            FROM market_products mp
                            JOIN products p ON mp.product_id = p.product_id
                            WHERE mp.market_id = %s
                            ORDER BY p.product_name
                        """, (row['market_id'],))
                        products = [r['product_name'] for r in cur.fetchall()]

                        # Оплата
                        cur.execute("""
                            SELECT py.payment_name
                            FROM market_payments mp
                            JOIN payment_methods py ON mp.payment_id = py.payment_id
                            WHERE mp.market_id = %s
                            ORDER BY py.payment_name
                        """, (row['market_id'],))
                        payments = [r['payment_name'] for r in cur.fetchall()]

                        # Соцсети
                        cur.execute("""
                            SELECT sn.social_networks, msl.url
                            FROM market_social_links msl
                            JOIN social_networks sn ON msl.social_network_id = sn.social_network_id
                            WHERE msl.market_id = %s
                            ORDER BY sn.social_networks
                        """, (row['market_id'],))
                        socials = [{"name": r['social_networks'], "url": r['url'] or "нет ссылки"} for r in cur.fetchall()]

                        # Отзывы
                        cur.execute("""
                            SELECT user_name, rating, review_text, created_at
                            FROM reviews
                            WHERE market_id = %s
                            ORDER BY created_at DESC
                        """, (row['market_id'],))
                        reviews = []
                        for r in cur.fetchall():
                            stars = "★" * r['rating'] + "☆" * (5 - r['rating'])
                            date_str = r['created_at'].strftime('%d.%m.%Y')
                            reviews.append({
                                "user": r['user_name'],
                                "stars": stars,
                                "rating": r['rating'],
                                "date": date_str,
                                "text": r['review_text']
                            })

                        market = {
                            "name": row['market_name'],
                            "address": f"{row['street']}, {row['city']}, {row['state']} {row['zip']}",
                            "coords": f"({row['x']}, {row['y']})",
                            "location": row['location'],
                            "products": products,
                            "payments": payments,
                            "socials": socials,
                            "reviews": reviews
                        }
                    else:
                        flash("Рынок не найден", "error")
            finally:
                conn.close()
    return render_template('detail.html', name=name, market=market)

@app.route('/feedback', methods=['GET', 'POST'])
@require_auth
def feedback_page():
    if request.method == 'POST':
        market_name = request.form.get('market_name', '').strip()
        user_name = request.form.get('user_name', '').strip()
        rating_str = request.form.get('rating', '').strip()
        review_text = request.form.get('review_text') or None

        if not market_name or not user_name:
            flash("Заполните все обязательные поля", "error")
        else:
            try:
                rating = int(rating_str)
                if not (1 <= rating <= 5):
                    raise ValueError
            except ValueError:
                flash("Рейтинг должен быть числом от 1 до 5", "error")
            else:
                conn = get_db_connection()
                if conn:
                    try:
                        with conn.cursor() as cur:
                            cur.execute("SELECT market_id FROM farmers_markets WHERE LOWER(TRIM(market_name)) = %s", (market_name.lower(),))
                            market = cur.fetchone()
                            if not market:
                                flash("Рынок не найден", "error")
                            else:
                                cur.execute("""
                                    INSERT INTO reviews (market_id, user_name, rating, review_text)
                                    VALUES (%s, %s, %s, %s)
                                """, (market['market_id'], user_name, rating, review_text))
                                conn.commit()
                                flash("✅ Отзыв успешно добавлен!", "success")
                    except Exception as e:
                        conn.rollback()
                        flash(f"Ошибка отправки отзыва: {e}", "error")
                    finally:
                        conn.close()
    return render_template('feedback.html')

@app.route('/delete', methods=['GET', 'POST'])
@require_auth
def delete_page():
    if request.method == 'POST':
        market_name = request.form.get('market_name', '').strip()
        if not market_name:
            flash("Введите название рынка", "error")
        else:
            conn = get_db_connection()
            if conn:
                try:
                    with conn.cursor() as cur:
                        cur.execute("DELETE FROM farmers_markets WHERE LOWER(TRIM(market_name)) = %s RETURNING market_id", (market_name.lower(),))
                        if cur.fetchone():
                            conn.commit()
                            flash(f"✅ Рынок '{market_name}' удалён.", "success")
                        else:
                            flash("❌ Рынок не найден.", "error")
                except Exception as e:
                    conn.rollback()
                    flash(f"Ошибка удаления: {e}", "error")
                finally:
                    conn.close()
    return render_template('delete.html')

@app.route('/add_market', methods=['GET', 'POST'])
@require_auth
def add_market():
    if request.method == 'GET':
        conn = get_db_connection()
        if not conn:
            flash("Ошибка подключения к БД", "error")
            return redirect(url_for('login'))

        try:
            with conn.cursor() as cur:
                cur.execute("SELECT product_id, product_name FROM products ORDER BY product_name")
                products = cur.fetchall()

                cur.execute("SELECT payment_id, payment_name FROM payment_methods ORDER BY payment_name")
                payments = cur.fetchall()

                cur.execute("SELECT social_network_id, social_networks FROM social_networks ORDER BY social_networks")
                social_networks = cur.fetchall()

            return render_template('add_market.html',
                                   products=products,
                                   payments=payments,
                                   social_networks=social_networks)
        finally:
            conn.close()

    market_name = request.form.get('market_name', '').strip()
    street = request.form.get('street', '').strip()
    city = request.form.get('city', '').strip()
    state = request.form.get('state', '').strip()
    zip_code = request.form.get('zip', '').strip()
    x_str = request.form.get('x', '').strip()
    y_str = request.form.get('y', '').strip()
    location = request.form.get('location', '').strip()

    if not all([market_name, street, city, state, zip_code]):
        flash("Все поля кроме координат и описания местоположения обязательны.", "error")
        return redirect(url_for('add_market'))

    x = y = None
    if x_str or y_str:
        try:
            x = float(x_str) if x_str else None
            y = float(y_str) if y_str else None
            if (x is None) != (y is None):  # XOR: только один указан
                flash("Укажите обе координаты или оставьте обе пустыми.", "error")
                return redirect(url_for('add_market'))
        except ValueError:
            flash("Координаты должны быть числами.", "error")
            return redirect(url_for('add_market'))

    product_ids = request.form.getlist('products')
    payment_ids = request.form.getlist('payments')
    social_ids = request.form.getlist('social_networks')
    social_urls = request.form.getlist('social_urls')

    conn = get_db_connection()
    if not conn:
        flash("Ошибка подключения к БД", "error")
        return redirect(url_for('add_market'))

    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO farmers_markets (market_name, street, city, state, zip, x, y, location)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                RETURNING market_id
            """, (market_name, street, city, state, zip_code, x, y, location))
            market_id = cur.fetchone()['market_id']

            for pid in product_ids:
                cur.execute("INSERT INTO market_products (market_id, product_id) VALUES (%s, %s)", (market_id, int(pid)))

            for pid in payment_ids:
                cur.execute("INSERT INTO market_payments (market_id, payment_id) VALUES (%s, %s)", (market_id, int(pid)))

            if len(social_ids) == len(social_urls):
                for sn_id, url in zip(social_ids, social_urls):
                    url_clean = url.strip() or None
                    cur.execute("""
                        INSERT INTO market_social_links (market_id, social_network_id, url)
                        VALUES (%s, %s, %s)
                    """, (market_id, int(sn_id), url_clean))

            conn.commit()
            flash(f"✅ Рынок '{market_name}' успешно добавлен!", "success")
            return redirect(url_for('markets'))

    except Exception as e:
        conn.rollback()
        flash(f"Ошибка добавления рынка: {e}", "error")
        return redirect(url_for('add_market'))
    finally:
        conn.close()

@app.route('/import_markets', methods=['GET', 'POST'])
@require_auth
def import_markets():
    if request.method == 'GET':
        return render_template('import_markets.html')

    file = request.files.get('excel_file')
    if not file or not file.filename.endswith(('.xlsx', '.xls')):
        flash("Пожалуйста, загрузите файл Excel (.xlsx)", "error")
        return redirect(url_for('import_markets'))

    filename = file.filename
    user_ip = request.environ.get('HTTP_X_REAL_IP') or request.remote_addr
    operation_type = 'import'

    conn = get_db_connection()
    if not conn:
        flash("Ошибка подключения к БД", "error")
        return redirect(url_for('import_markets'))

    try:
        # Сохраняем временный файл
        with tempfile.NamedTemporaryFile(delete=False, suffix='.xlsx') as tmp:
            file.save(tmp.name)
            tmp_path = tmp.name

        # Читаем Excel
        df = pd.read_excel(tmp_path, dtype=str).fillna('')
        # После успешного импорта — сохраняем исходный файл в MinIO
        save_file_to_minio_and_log(tmp_path, filename, operation_type, user_ip)
        os.unlink(tmp_path)  # удаляем временный файл

        # Обязательные колонки
        required_cols = {'market_name', 'street', 'city', 'state', 'zip'}
        if not required_cols.issubset(df.columns):
            missing = required_cols - set(df.columns)
            flash(f"В файле отсутствуют обязательные колонки: {', '.join(missing)}", "error")
            return redirect(url_for('import_markets'))

        added = 0
        errors = []

        with conn.cursor() as cur:
            cur.execute("SELECT product_id, product_name FROM products")
            products_map = {row['product_name'].strip().lower(): row['product_id'] for row in cur.fetchall()}

            cur.execute("SELECT payment_id, payment_name FROM payment_methods")
            payments_map = {row['payment_name'].strip().lower(): row['payment_id'] for row in cur.fetchall()}

            cur.execute("SELECT social_network_id, social_networks FROM social_networks")
            socials_map = {row['social_networks'].strip().lower(): row['social_network_id'] for row in cur.fetchall()}

        with conn.cursor() as cur:
            for idx, row in df.iterrows():
                try:
                    market_name = row['market_name'].strip()
                    street = row['street'].strip()
                    city = row['city'].strip()
                    state = row['state'].strip()
                    zip_code = row['zip'].strip()
                    location = row.get('location', '').strip()

                    # Координаты
                    x = y = None
                    if 'x' in row and row['x']:
                        try:
                            x = float(row['x'])
                        except (ValueError, TypeError):
                            pass
                    if 'y' in row and row['y']:
                        try:
                            y = float(row['y'])
                        except (ValueError, TypeError):
                            pass

                    # Вставка рынка
                    cur.execute("""
                        INSERT INTO farmers_markets (market_name, street, city, state, zip, x, y, location)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                        RETURNING market_id
                    """, (market_name, street, city, state, zip_code, x, y, location))
                    market_id = cur.fetchone()['market_id']

                    # Обработка продуктов
                    if 'products' in row and row['products']:
                        for p_name in row['products'].split(','):
                            p_key = p_name.strip().lower()
                            if p_key in products_map:
                                cur.execute("INSERT INTO market_products (market_id, product_id) VALUES (%s, %s)",
                                            (market_id, products_map[p_key]))

                    # Обработка оплаты
                    if 'payments' in row and row['payments']:
                        for p_name in row['payments'].split(','):
                            p_key = p_name.strip().lower()
                            if p_key in payments_map:
                                cur.execute("INSERT INTO market_payments (market_id, payment_id) VALUES (%s, %s)",
                                            (market_id, payments_map[p_key]))

                    # Обработка соцсетей
                    if 'socials' in row and row['socials']:
                        for item in row['socials'].split(','):
                            item = item.strip()
                            if ':' in item:
                                sn_name, url = item.split(':', 1)
                                sn_key = sn_name.strip().lower()
                                url = url.strip() or None
                                if sn_key in socials_map:
                                    cur.execute("""
                                        INSERT INTO market_social_links (market_id, social_network_id, url)
                                        VALUES (%s, %s, %s)
                                    """, (market_id, socials_map[sn_key], url))

                    added += 1

                except Exception as e:
                    errors.append(f"Строка {idx + 2}: {str(e)[:100]}")

            conn.commit()

        if errors:
            flash(f"✅ Добавлено рынков: {added}. Ошибки ({len(errors)}):<br>" + "<br>".join(errors), "error")
        else:
            flash(f"✅ Успешно добавлено {added} рынков!", "success")

        return redirect(url_for('markets'))

    except Exception as e:
        flash(f"Ошибка обработки файла: {e}", "error")
        return redirect(url_for('import_markets'))
    finally:
        conn.close()


@app.route('/download_template')
@require_auth
def download_template():
    # Создаём шаблон Excel
    template_data = {
        'market_name': ['Центральный рынок', 'Зелёный базар'],
        'street': ['Ленина, 10', 'Гагарина, 25'],
        'city': ['Москва', 'Санкт-Петербург'],
        'state': ['Москва', 'СПб'],
        'zip': ['101000', '190000'],
        'x': [-73.994454, -74.006015],
        'y': [40.750042, 40.712728],
        'location': ['У фонтана', 'Рядом с метро'],
        'products': ['Овощи, Фрукты, Мёд', 'Молоко, Хлеб, Сыр'],
        'payments': ['Наличные, Карта', 'Карта, Apple Pay'],
        'socials': ['Instagram:https://instagram.com/market1, ВКонтакте:', 'Telegram:@market2']
    }
    df = pd.DataFrame(template_data)

    with tempfile.NamedTemporaryFile(delete=False, suffix='.xlsx') as tmp:
        df.to_excel(tmp.name, index=False, sheet_name='Рынки')
        tmp_path = tmp.name

    return send_file(tmp_path, as_attachment=True, download_name="шаблон_рынков.xlsx")

@app.route('/edit_market', methods=['GET', 'POST'])
@require_auth
def edit_market():
    conn = get_db_connection()
    if not conn:
        flash("Ошибка подключения к БД", "error")
        return redirect(url_for('login'))

    try:
        with conn.cursor() as cur:
            cur.execute("SELECT product_id, product_name FROM products ORDER BY product_name")
            all_products = cur.fetchall()

            cur.execute("SELECT payment_id, payment_name FROM payment_methods ORDER BY payment_name")
            all_payments = cur.fetchall()

            cur.execute("SELECT social_network_id, social_networks FROM social_networks ORDER BY social_networks")
            all_socials = cur.fetchall()

        if request.method == 'GET':
            market_name = request.args.get('name', '').strip()
            market = None
            if market_name:
                with conn.cursor() as cur:
                    cur.execute("""
                        SELECT * FROM farmers_markets
                        WHERE LOWER(TRIM(market_name)) = %s
                    """, (market_name.lower(),))
                    row = cur.fetchone()
                    if row:
                        # Текущие продукты
                        cur.execute("""
                            SELECT product_id FROM market_products WHERE market_id = %s
                        """, (row['market_id'],))
                        selected_products = {r['product_id'] for r in cur.fetchall()}

                        # Текущие способы оплаты
                        cur.execute("""
                            SELECT payment_id FROM market_payments WHERE market_id = %s
                        """, (row['market_id'],))
                        selected_payments = {r['payment_id'] for r in cur.fetchall()}

                        # Текущие соцсети
                        cur.execute("""
                            SELECT social_network_id, url FROM market_social_links WHERE market_id = %s
                        """, (row['market_id'],))
                        social_links = {r['social_network_id']: r['url'] or '' for r in cur.fetchall()}

                        market = {
                            'market_id': row['market_id'],
                            'market_name': row['market_name'],
                            'street': row['street'],
                            'city': row['city'],
                            'state': row['state'],
                            'zip': row['zip'],
                            'x': row['x'],
                            'y': row['y'],
                            'location': row['location'] or '',
                            'selected_products': selected_products,
                            'selected_payments': selected_payments,
                            'social_links': social_links
                        }
                    else:
                        flash("Рынок не найден", "error")

            return render_template('edit_market.html',
                                   market=market,
                                   products=all_products,
                                   payments=all_payments,
                                   social_networks=all_socials)

        market_id = request.form.get('market_id')
        if not market_id:
            flash("Не указан ID рынка", "error")
            return redirect(url_for('edit_market'))

        street = request.form.get('street', '').strip()
        city = request.form.get('city', '').strip()
        state = request.form.get('state', '').strip()
        zip_code = request.form.get('zip', '').strip()

        if not all([street, city, state, zip_code]):
            flash("Поля адреса (улица, город, субъект, индекс) обязательны.", "error")
            return redirect(url_for('edit_market', name=request.form.get('original_name', '')))

        x_str = request.form.get('x', '').strip()
        y_str = request.form.get('y', '').strip()
        location = request.form.get('location', '').strip()

        x = y = None
        if x_str or y_str:
            try:
                x = float(x_str) if x_str else None
                y = float(y_str) if y_str else None
                if (x is None) != (y is None):
                    flash("Укажите обе координаты или оставьте обе пустыми.", "error")
                    return redirect(url_for('edit_market', name=request.form.get('original_name', '')))
            except ValueError:
                flash("Координаты должны быть числами.", "error")
                return redirect(url_for('edit_market', name=request.form.get('original_name', '')))

        # Получаем выбранные ID
        product_ids = [int(pid) for pid in request.form.getlist('products') if pid.isdigit()]
        payment_ids = [int(pid) for pid in request.form.getlist('payments') if pid.isdigit()]
        social_ids = [int(sid) for sid in request.form.getlist('social_networks') if sid.isdigit()]
        social_urls = request.form.getlist('social_urls')

        with conn.cursor() as cur:
            # Обновляем основную запись
            cur.execute("""
                UPDATE farmers_markets
                SET street = %s, city = %s, state = %s, zip = %s, x = %s, y = %s, location = %s
                WHERE market_id = %s
            """, (street, city, state, zip_code, x, y, location, market_id))

            # Удаляем старые связи
            cur.execute("DELETE FROM market_products WHERE market_id = %s", (market_id,))
            cur.execute("DELETE FROM market_payments WHERE market_id = %s", (market_id,))
            cur.execute("DELETE FROM market_social_links WHERE market_id = %s", (market_id,))

            # Добавляем новые связи
            for pid in product_ids:
                cur.execute("INSERT INTO market_products (market_id, product_id) VALUES (%s, %s)", (market_id, pid))

            for pid in payment_ids:
                cur.execute("INSERT INTO market_payments (market_id, payment_id) VALUES (%s, %s)", (market_id, pid))

            if len(social_ids) == len(social_urls):
                for sn_id, url in zip(social_ids, social_urls):
                    url_clean = url.strip() or None
                    cur.execute("""
                        INSERT INTO market_social_links (market_id, social_network_id, url)
                        VALUES (%s, %s, %s)
                    """, (market_id, sn_id, url_clean))

            conn.commit()
            flash("✅ Рынок успешно обновлён!", "success")
            return redirect(url_for('markets'))

    except Exception as e:
        conn.rollback()
        flash(f"Ошибка обновления: {e}", "error")
        return redirect(url_for('edit_market', name=request.form.get('original_name', '')) if request.method == 'POST' else url_for('edit_market'))
    finally:
        conn.close()

# Основной шрифт для текста
dejavu_path = None
try:
    import reportlab
    dejavu_path = os.path.join(os.path.dirname(__file__), 'static', 'fonts', 'DejaVuSans.ttf')
    if not os.path.exists(dejavu_path):
        dejavu_path = None
except:
    dejavu_path = None

# Шрифт для смайликов
noto_emoji_path = os.path.join(os.path.dirname(__file__), 'static', 'fonts', 'NotoEmoji-Regular.ttf')

# Регистрируем шрифты
if dejavu_path and os.path.exists(dejavu_path):
    pdfmetrics.registerFont(TTFont('DejaVu', dejavu_path))
    base_font = 'DejaVu'
else:
    base_font = 'Helvetica'

if os.path.exists(noto_emoji_path):
    pdfmetrics.registerFont(TTFont('NotoEmoji', noto_emoji_path))
    emoji_font = 'NotoEmoji'
else:
    emoji_font = base_font

@app.route('/download_pdf')
@require_auth
def download_pdf():
    market_name = request.args.get('name', '').strip()
    if not market_name:
        flash("Не указано название рынка", "error")
        return redirect(url_for('detail_page'))

    user_ip = request.environ.get('HTTP_X_REAL_IP') or request.remote_addr
    operation_type = 'pdf_export'

    conn = get_db_connection()
    if not conn:
        flash("Ошибка подключения к БД", "error")
        return redirect(url_for('detail_page'))

    try:
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM farmers_markets WHERE LOWER(TRIM(market_name)) = %s", (market_name.lower(),))
            row = cur.fetchone()
            if not row:
                flash("Рынок не найден", "error")
                return redirect(url_for('detail_page'))

            # Сбор данных
            cur.execute("SELECT p.product_name FROM market_products mp JOIN products p ON mp.product_id = p.product_id WHERE mp.market_id = %s ORDER BY p.product_name", (row['market_id'],))
            products = [r['product_name'] for r in cur.fetchall()]

            cur.execute("SELECT py.payment_name FROM market_payments mp JOIN payment_methods py ON mp.payment_id = py.payment_id WHERE mp.market_id = %s ORDER BY py.payment_name", (row['market_id'],))
            payments = [r['payment_name'] for r in cur.fetchall()]

            cur.execute("SELECT sn.social_networks, msl.url FROM market_social_links msl JOIN social_networks sn ON msl.social_network_id = sn.social_network_id WHERE msl.market_id = %s ORDER BY sn.social_networks", (row['market_id'],))
            socials = [{"name": r['social_networks'], "url": r['url'] or "нет ссылки"} for r in cur.fetchall()]

            cur.execute("SELECT user_name, rating, review_text, created_at FROM reviews WHERE market_id = %s ORDER BY created_at DESC", (row['market_id'],))
            reviews = []
            for r in cur.fetchall():
                stars = "★" * r['rating'] + "☆" * (5 - r['rating'])
                date_str = r['created_at'].strftime('%d.%m.%Y')
                reviews.append({"user": r['user_name'], "stars": stars, "rating": r['rating'], "date": date_str, "text": r['review_text'] or ""})

            market = {
                "name": row['market_name'],
                "address": f"{row['street']}, {row['city']}, {row['state']} {row['zip']}",
                "coords": f"({row['x']}, {row['y']})" if row['x'] is not None and row['y'] is not None else "не указаны",
                "location": row['location'] or "—",
                "products": products,
                "payments": payments,
                "socials": socials,
                "reviews": reviews
            }

        # Генерация PDF с эмодзи
        buffer = io.BytesIO()
        doc = SimpleDocTemplate(buffer, pagesize=A4, topMargin=0.8*inch, bottomMargin=0.6*inch)
        styles = getSampleStyleSheet()

        # Стиль для обычного текста
        normal_style = ParagraphStyle(
            'Normal',
            fontName=base_font,
            fontSize=11,
            leading=14
        )

        # Стиль для заголовков со смайликами
        def make_mixed_text(text):
            """Заменяет эмодзи на <font> с другим шрифтом"""
            import re
            emoji_pattern = re.compile(
                "["
                "\U0001F600-\U0001F64F"  # эмоции
                "\U0001F300-\U0001F5FF"  # символы и пиктограммы
                "\U0001F680-\U0001F6FF"  # транспорт
                "\U0001F1E0-\U0001F1FF"  # флаги
                "\U00002702-\U000027B0"  # другие
                "\U000024C2-\U0001F251" 
                "]+", flags=re.UNICODE
            )
            parts = []
            last_end = 0
            for match in emoji_pattern.finditer(text):
                if match.start() > last_end:
                    parts.append(f'<font name="{base_font}">{text[last_end:match.start()]}</font>')
                parts.append(f'<font name="{emoji_font}">{match.group()}</font>')
                last_end = match.end()
            if last_end < len(text):
                parts.append(f'<font name="{base_font}">{text[last_end:]}</font>')
            return ''.join(parts)

        title = make_mixed_text("🌾 Фермерские рынки")
        heading1_style = ParagraphStyle('Heading1', fontName=base_font, fontSize=16, alignment=TA_CENTER)
        story = [Paragraph(title, heading1_style), Spacer(1, 12)]

        story.append(Paragraph(market['name'], ParagraphStyle('H2', fontName=base_font, fontSize=14)))
        story.append(Spacer(1, 12))

        info_lines = [
            make_mixed_text(f"📍 Адрес: {market['address']}"),
            make_mixed_text(f"🌐 Координаты: {market['coords']}"),
            make_mixed_text(f"📌 Местоположение: {market['location']}")
        ]
        for line in info_lines:
            story.append(Paragraph(line, normal_style))
            story.append(Spacer(1, 6))

        story.append(Spacer(1, 12))

        def add_section(title_text, items):
            if items:
                title_with_emoji = make_mixed_text(title_text)
                story.append(Paragraph(title_with_emoji, normal_style))
                for item in items:
                    story.append(Paragraph(f" • {item}", normal_style))
                story.append(Spacer(1, 8))

        add_section("🍎 Продукты", market['products'])
        add_section("💳 Способы оплаты", market['payments'])

        if market['socials']:
            title = make_mixed_text("🌐 Социальные сети")
            story.append(Paragraph(title, normal_style))
            for s in market['socials']:
                story.append(Paragraph(f" • {s['name']}: {s['url']}", normal_style))
            story.append(Spacer(1, 8))

        if market['reviews']:
            title = make_mixed_text("💬 Отзывы")
            story.append(Paragraph(title, normal_style))
            for r in market['reviews']:
                review_text = f"[{r['user']}] {r['stars']} ({r['date']})"
                if r['text']:
                    review_text += f"<br/>&nbsp;&nbsp;&nbsp;&nbsp;«{r['text']}»"
                story.append(Paragraph(review_text, normal_style))
                story.append(Spacer(1, 6))
        else:
            story.append(Paragraph(make_mixed_text("💬 Отзывов нет."), normal_style))
            story.append(Spacer(1, 8))

        story.append(Spacer(1, 24))
        moscow_time = datetime.now(ZoneInfo("Europe/Moscow"))
        now = moscow_time.strftime("%d.%m.%Y %H:%M:%S")
        stamp_text = make_mixed_text(f"Информация верна на {now}")
        stamp_style = ParagraphStyle(
            'Stamp',
            fontName=base_font,
            fontSize=10,
            alignment=TA_CENTER,
            textColor=colors.grey,
            borderWidth=1,
            borderColor=colors.grey,
            borderPadding=8,
            borderRadius=5
        )
        story.append(Paragraph(stamp_text, stamp_style))

        doc.build(story)
        buffer.seek(0)

        # Сохраняем PDF во временный файл
        original_filename = f"Рынок_{market_name.replace(' ', '_')}.pdf"
        with tempfile.NamedTemporaryFile(delete=False, suffix='.pdf') as tmp:
            tmp.write(buffer.getvalue())
            tmp_path = tmp.name

        # Сохраняем в MinIO и логируем
        save_file_to_minio_and_log(tmp_path, original_filename, operation_type, user_ip)

        # Отправляем пользователю
        response = send_file(tmp_path, as_attachment=True, download_name=original_filename)

        @response.call_on_close
        def remove_file():
            try:
                os.unlink(tmp_path)
            except:
                pass

        return response

    except Exception as e:
        flash(f"Ошибка генерации PDF: {e}", "error")
        return redirect(url_for('detail_page', name=market_name))
    finally:
        conn.close()

@app.route('/export_all')
@require_auth
def export_all():
    user_ip = request.environ.get('HTTP_X_REAL_IP') or request.remote_addr
    operation_type = 'export'

    engine = create_engine(
        f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    )

    try:
        with tempfile.NamedTemporaryFile(delete=False, suffix='.xlsx') as tmp:
            tmp_path = tmp.name

        # Создаём Excel
        workbook = xlsxwriter.Workbook(tmp_path, {'constant_memory': True})
        worksheet = workbook.add_worksheet('Рынки')

        with engine.connect() as conn:
            result = conn.execution_options(stream_results=True).execute(
                text("SELECT * FROM mv_markets_export ORDER BY market_name")
            )
            columns = result.keys()
            worksheet.write_row(0, 0, columns)

            row_num = 1
            while True:
                chunk = result.fetchmany(1000)
                if not chunk:
                    break
                for row in chunk:
                    worksheet.write_row(row_num, 0, row)
                    row_num += 1

        workbook.close()

        original_filename = f"все_рынки_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
        save_file_to_minio_and_log(tmp_path, original_filename, operation_type, user_ip)

        return send_file(tmp_path, as_attachment=True, download_name=original_filename)

    except Exception as e:
        flash(f"Ошибка экспорта: {e}", "error")
        return redirect(url_for('markets'))
    finally:
        try:
            os.unlink(tmp_path)
        except:
            pass

@app.route('/stats')
@require_auth
def stats():
    conn = get_db_connection()
    if not conn:
        flash("Ошибка подключения к БД", "error")
        return redirect(url_for('login'))

    try:
        with conn.cursor() as cur:
            # Общая статистика
            cur.execute("SELECT COUNT(*) AS total FROM farmers_markets")
            total_markets = cur.fetchone()['total']

            cur.execute("SELECT COUNT(*) AS total FROM reviews")
            total_reviews = cur.fetchone()['total']

            cur.execute("SELECT COUNT(*) AS total FROM products")
            total_products = cur.fetchone()['total']

            cur.execute("SELECT COUNT(*) AS total FROM payment_methods")
            total_payments = cur.fetchone()['total']

            cur.execute("SELECT COUNT(*) AS total FROM social_networks")
            total_socials = cur.fetchone()['total']

            # Средний рейтинг
            cur.execute("""
                SELECT COALESCE(ROUND(AVG(rating), 2), 0) AS avg_rating
                FROM reviews
            """)
            avg_rating = float(cur.fetchone()['avg_rating'])

            # Топ-5 рынков по рейтингу
            cur.execute("""
                SELECT fm.market_name, fm.city, fm.state, 
                       COALESCE(ROUND(AVG(r.rating), 2), 0) AS avg_rating,
                       COUNT(r.review_id) AS review_count
                FROM farmers_markets fm
                LEFT JOIN reviews r ON fm.market_id = r.market_id
                GROUP BY fm.market_id, fm.market_name, fm.city, fm.state
                HAVING COUNT(r.review_id) > 0
                ORDER BY avg_rating DESC, review_count DESC
                LIMIT 5
            """)
            top_markets = cur.fetchall()

            # Рынки по субъектам (топ-10)
            cur.execute("""
                SELECT state, COUNT(*) AS count
                FROM farmers_markets
                GROUP BY state
                ORDER BY count DESC
                LIMIT 10
            """)
            markets_by_state = cur.fetchall()

            stats_data = {
                'total_markets': total_markets,
                'total_reviews': total_reviews,
                'avg_rating': avg_rating,
                'total_products': total_products,
                'total_payments': total_payments,
                'total_socials': total_socials,
                'top_markets': top_markets,
                'markets_by_state': markets_by_state
            }

        return render_template('stats.html', stats=stats_data)

    except Exception as e:
        flash(f"Ошибка загрузки статистики: {e}", "error")
        return redirect(url_for('markets'))
    finally:
        conn.close()

@app.route('/help')
@require_auth
def help_page():
    return render_template('help.html')

if __name__ == '__main__':
    app.run(debug=False)