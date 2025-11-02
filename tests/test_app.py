# tests/test_app.py
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from app.app import haversine, app

import bcrypt
from unittest.mock import patch, MagicMock
from flask import session
from datetime import datetime

import io
import pandas as pd
from io import BytesIO

def test_haversine_same_point():
    """Расстояние между одной и той же точкой — 0."""
    assert haversine(55.7558, 37.6176, 55.7558, 37.6176) == 0.0

def test_haversine_known_distance():
    """Тест на приблизительное расстояние между Москвой и Санкт-Петербургом (в милях)."""
    moscow_lat, moscow_lon = 55.7558, 37.6176
    spb_lat, spb_lon = 59.9343, 30.3351
    distance = haversine(moscow_lat, moscow_lon, spb_lat, spb_lon)
    # Фактическое расстояние ≈ 393.35 миль
    assert 390 < distance < 397

def test_haversine_equator():
    """Тест: 1 градус долготы на экваторе ≈ 69 миль."""
    # На экваторе (широта 0), 1° долготы ≈ 69 миль
    dist = haversine(0.0, 0.0, 0.0, 1.0)
    assert 68 < dist < 70

def test_login_page_renders():
    """GET / → отображается форма входа"""
    with app.test_client() as client:
        response = client.get('/')
        assert response.status_code == 200
        # Проверяем наличие ключевых элементов формы
        html = response.get_data(as_text=True)
        assert 'name="username"' in html
        assert 'name="password"' in html
        assert 'Войти' in html  # или '<button'

@patch('app.app.get_db_connection')
def test_login_success(mock_get_db):
    """Успешный вход с правильными учётными данными"""
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_get_db.return_value = mock_conn
    mock_conn.cursor.return_value.__enter__.return_value = mock_cursor

    password = "secret123"
    hashed = bcrypt.hashpw(password.encode('utf-8'), bcrypt.gensalt()).decode('utf-8')

    mock_cursor.fetchone.return_value = {
        'id': 1,
        'username': 'admin',
        'password_hash': hashed,
        'is_admin': True
    }

    with app.test_client() as client:
        response = client.post('/', data={'username': 'admin', 'password': password})
        assert response.status_code == 302
        assert response.location.endswith('/markets')

        # Теперь session доступна благодаря импорту
        assert session['authenticated'] is True
        assert session['is_admin'] is True
        assert session['username'] == 'admin'

@patch('app.app.get_db_connection')
def test_login_invalid_password(mock_get_db):
    """Неверный пароль → ошибка"""
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_get_db.return_value = mock_conn
    mock_conn.cursor.return_value.__enter__.return_value = mock_cursor

    hashed = bcrypt.hashpw("correct_pass".encode('utf-8'), bcrypt.gensalt()).decode('utf-8')
    mock_cursor.fetchone.return_value = {
        'id': 1,
        'username': 'user1',
        'password_hash': hashed,
        'is_admin': False
    }

    with app.test_client() as client:
        response = client.post('/', data={'username': 'user1', 'password': 'wrong_pass'})
        assert response.status_code == 200
        assert 'Неверный логин или пароль' in response.get_data(as_text=True)

def test_login_empty_fields():
    """Пустые поля → ошибка"""
    with app.test_client() as client:
        response = client.post('/', data={'username': '', 'password': ''})
        assert response.status_code == 200
        assert 'Введите логин и пароль' in response.get_data(as_text=True)

@patch('app.app.get_db_connection')
def test_login_db_error(mock_get_db):
    """Ошибка подключения к БД → сообщение об ошибке"""
    mock_get_db.return_value = None  # имитируем провал подключения

    with app.test_client() as client:
        response = client.post('/', data={'username': 'test', 'password': 'pass'})
        assert response.status_code == 200
        assert 'Ошибка подключения к БД' in response.get_data(as_text=True)

@patch('app.app.get_db_connection')
def test_markets_requires_auth(mock_get_db):
    """Доступ без авторизации → редирект на /login"""
    with app.test_client() as client:
        response = client.get('/markets', follow_redirects=False)
        assert response.status_code == 302
        assert response.location.endswith('/')


def test_markets_redirects_if_not_authenticated():
    """Проверка: без сессии — редирект"""
    with app.test_client() as client:
        response = client.get('/markets')
        assert response.status_code == 302
        assert '/login' in response.location or response.location == '/'


@patch('app.app.get_db_connection')
def test_markets_db_error(mock_get_db):
    """Ошибка подключения к БД → flash + редирект на login"""
    mock_get_db.return_value = None

    with app.test_client() as client:
        # Симулируем вход
        with client.session_transaction() as sess:
            sess['authenticated'] = True
            sess['is_admin'] = False
            sess['username'] = 'user'

        response = client.get('/markets')
        assert response.status_code == 302
        # После flash — редирект на login
        assert response.location.endswith('/')


@patch('app.app.get_db_connection')
def test_markets_empty_db(mock_get_db):
    """Пустая таблица → отображается пустой список"""
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_get_db.return_value = mock_conn
    mock_conn.cursor.return_value.__enter__.return_value = mock_cursor

    # COUNT = 0
    mock_cursor.fetchone.side_effect = [
        {'total': 0},  # для COUNT(*)
        []             # для SELECT (fetchall)
    ]

    with app.test_client() as client:
        with client.session_transaction() as sess:
            sess['authenticated'] = True
            sess['is_admin'] = False
            sess['username'] = 'user'

        response = client.get('/markets')
        assert response.status_code == 200
        html = response.get_data(as_text=True)
        assert 'Нет данных' in html or 'markets' in html  # зависит от шаблона
        # Проверим, что total_pages = 0 или 1
        # (в коде: total_pages = (0 + 10 - 1) // 10 = 0)


@patch('app.app.get_db_connection')
def test_markets_with_data(mock_get_db):
    """Корректное отображение списка рынков"""
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_get_db.return_value = mock_conn
    mock_conn.cursor.return_value.__enter__.return_value = mock_cursor

    mock_cursor.fetchone.return_value = {'total': 2}
    mock_cursor.fetchall.return_value = [
        {
            'market_name': 'Центральный рынок',
            'city': 'Москва',
            'state': 'Москва',
            'avg_rating': 4.5,
            'review_count': 10
        },
        {
            'market_name': 'Зелёный базар',
            'city': 'СПб',
            'state': 'СПб',
            'avg_rating': 3.8,
            'review_count': 5
        }
    ]

    with app.test_client() as client:
        with client.session_transaction() as sess:
            sess['authenticated'] = True
            sess['is_admin'] = False
            sess['username'] = 'user'

        response = client.get('/markets')
        assert response.status_code == 200
        html = response.get_data(as_text=True)
        assert 'Центральный рынок' in html
        assert 'Зелёный базар' in html
        assert '★' in html  # звёзды рейтинга


@patch('app.app.get_db_connection')
def test_markets_invalid_page(mock_get_db):
    """Некорректный номер страницы → исправляется на 1"""
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_get_db.return_value = mock_conn
    mock_conn.cursor.return_value.__enter__.return_value = mock_cursor

    mock_cursor.fetchone.return_value = {'total': 5}
    mock_cursor.fetchall.return_value = []  # не важно для редиректа

    with app.test_client() as client:
        with client.session_transaction() as sess:
            sess['authenticated'] = True

        # Запрос с page=999 → должен редиректнуть на последнюю страницу (1)
        response = client.get('/markets?page=999', follow_redirects=False)
        assert response.status_code == 302
        assert 'page=1' in response.location

@patch('app.app.get_db_connection')
def test_search_requires_auth(mock_get_db):
    """Попытка доступа без авторизации → редирект на /login"""
    with app.test_client() as client:
        response = client.get('/search', follow_redirects=False)
        assert response.status_code == 302
        assert response.location.endswith('/')


@patch('app.app.get_db_connection')
def test_search_empty_query_non_radius(mock_get_db):
    """Поиск без радиуса и без q → flash 'введите значение'"""
    with app.test_client() as client:
        with client.session_transaction() as sess:
            sess['authenticated'] = True

        response = client.get('/search?mode=city')
        assert response.status_code == 200
        assert 'Для поиска по городу/субъекту/индексу введите значение' in response.get_data(as_text=True)


@patch('app.app.get_db_connection')
def test_search_by_city_success(mock_get_db):
    """Успешный поиск по городу"""
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_get_db.return_value = mock_conn
    mock_conn.cursor.return_value.__enter__.return_value = mock_cursor

    mock_cursor.fetchall.return_value = [
        {'market_name': 'Центральный рынок', 'city': 'Москва', 'state': 'Москва'},
        {'market_name': 'Овощной базар', 'city': 'Москва', 'state': 'Москва'}
    ]

    with app.test_client() as client:
        with client.session_transaction() as sess:
            sess['authenticated'] = True

        response = client.get('/search?mode=city&q=Москва')
        html = response.get_data(as_text=True)
        assert response.status_code == 200
        assert 'Центральный рынок' in html
        assert 'Овощной базар' in html


@patch('app.app.get_db_connection')
def test_search_by_radius_success(mock_get_db):
    """Успешный поиск по координатам и радиусу"""
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_get_db.return_value = mock_conn
    mock_conn.cursor.return_value.__enter__.return_value = mock_cursor

    # Возвращаем все рынки (хаверсин фильтрует внутри)
    mock_cursor.fetchall.return_value = [
        {'market_name': 'Рынок у моря', 'city': 'Сочи', 'state': 'Краснодарский край', 'lat': 43.5855, 'lon': 39.7231},
        {'market_name': 'Горный базар', 'city': 'Кисловодск', 'state': 'Ставропольский край', 'lat': 43.9167, 'lon': 42.7167}
    ]

    with app.test_client() as client:
        with client.session_transaction() as sess:
            sess['authenticated'] = True

        # Поиск в радиусе 100 миль от Сочи → должен найти "Рынок у моря"
        response = client.get('/search?radius=1&lat=43.5855&lon=39.7231&radius_val=100')
        html = response.get_data(as_text=True)
        assert response.status_code == 200
        assert 'Рынок у моря' in html
        assert 'Горный базар' not in html  # слишком далеко (~200+ миль)


@patch('app.app.get_db_connection')
def test_search_invalid_coords(mock_get_db):
    """Неверные координаты → flash ошибка"""
    with app.test_client() as client:
        with client.session_transaction() as sess:
            sess['authenticated'] = True

        response = client.get('/search?radius=1&lat=abc&lon=xyz&radius_val=50')
        assert response.status_code == 200
        assert 'Некорректные координаты или радиус' in response.get_data(as_text=True)


@patch('app.app.get_db_connection')
def test_search_sort_by_rating(mock_get_db):
    """Сортировка по рейтингу (sort=3)"""
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_get_db.return_value = mock_conn
    mock_conn.cursor.return_value.__enter__.return_value = mock_cursor

    # Первый вызов — основной SELECT
    mock_cursor.fetchall.side_effect = [
        [{'market_name': 'Маркет А', 'city': 'СПб', 'state': 'СПб'}],
        [{'market_name': 'Маркет А', 'avg_rating': 4.7}]
    ]
    mock_cursor.fetchone.return_value = {'avg_rating': 4.7}

    with app.test_client() as client:
        with client.session_transaction() as sess:
            sess['authenticated'] = True

        response = client.get('/search?mode=city&q=СПб&sort=3')
        html = response.get_data(as_text=True)
        assert response.status_code == 200
        assert 'Маркет А' in html
        # Проверим, что был второй запрос к БД для рейтинга
        assert mock_cursor.execute.call_count >= 2


@patch('app.app.get_db_connection')
def test_search_db_error(mock_get_db):
    mock_get_db.return_value = None

    with app.test_client() as client:
        with client.session_transaction() as sess:
            sess['authenticated'] = True

        response = client.get('/search?mode=city&q=Москва')
        assert response.status_code == 200
        assert 'Ошибка подключения к БД' in response.get_data(as_text=True)

@patch('app.app.get_db_connection')
def test_detail_requires_auth(mock_get_db):
    """Попытка доступа без авторизации → редирект на /login"""
    with app.test_client() as client:
        response = client.get('/detail?name=Test', follow_redirects=False)
        assert response.status_code == 302
        assert response.location.endswith('/')


@patch('app.app.get_db_connection')
def test_detail_market_not_found(mock_get_db):
    """Рынок не найден → flash сообщение"""
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_get_db.return_value = mock_conn
    mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
    mock_cursor.fetchone.return_value = None  # farmers_markets не вернул запись

    with app.test_client() as client:
        with client.session_transaction() as sess:
            sess['authenticated'] = True

        response = client.get('/detail?name=Неизвестный+рынок')
        assert response.status_code == 200
        assert 'Рынок не найден' in response.get_data(as_text=True)


@patch('app.app.get_db_connection')
def test_detail_success(mock_get_db):
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_get_db.return_value = mock_conn
    mock_conn.cursor.return_value.__enter__.return_value = mock_cursor

    # Подготовка данных
    market_row = {
        'market_id': 1,
        'market_name': 'Центральный рынок',
        'street': 'Ленина',
        'city': 'Москва',
        'state': 'Москва',
        'zip': '101000',
        'x': 37.6176,
        'y': 55.7558,
        'location': 'У фонтана'
    }
    products = [{'product_name': 'Овощи'}, {'product_name': 'Фрукты'}]
    payments = [{'payment_name': 'Наличные'}, {'payment_name': 'Карта'}]
    socials = [
        {'social_networks': 'Instagram', 'url': 'https://insta.com'},
        {'social_networks': 'ВКонтакте', 'url': None}
    ]
    reviews = [{
        'user_name': 'Иван',
        'rating': 5,
        'review_text': 'Отлично!',
        'created_at': datetime(2025, 1, 15)
    }]

    # Эмуляция последовательных вызовов fetchone/fetchall
    def execute_side_effect(query, params=None):
        if 'farmers_markets' in query and 'WHERE' in query:
            mock_cursor.fetchone.return_value = market_row
        elif 'market_products' in query:
            mock_cursor.fetchall.return_value = products
        elif 'market_payments' in query:
            mock_cursor.fetchall.return_value = payments
        elif 'market_social_links' in query:
            mock_cursor.fetchall.return_value = socials
        elif 'reviews' in query:
            mock_cursor.fetchall.return_value = reviews
        else:
            mock_cursor.fetchone.return_value = None
            mock_cursor.fetchall.return_value = []

    mock_cursor.execute.side_effect = execute_side_effect

    with app.test_client() as client:
        with client.session_transaction() as sess:
            sess['authenticated'] = True

        response = client.get('/detail?name=Центральный+рынок')
        html = response.get_data(as_text=True)
        assert response.status_code == 200
        assert 'Центральный рынок' in html
        assert 'Ленина' in html
        assert 'Москва' in html
        assert '101000' in html
        assert 'Овощи' in html
        assert 'Наличные' in html
        assert 'Instagram' in html
        assert 'https://insta.com' in html
        assert 'ВКонтакте' in html
        assert 'нет ссылки' in html  # для None URL
        assert 'Иван' in html
        assert '★★★★★' in html
        assert '15.01.2025' in html


@patch('app.app.get_db_connection')
def test_detail_db_error(mock_get_db):
    """При ошибке подключения к БД — страница отдаётся с market=None"""
    mock_get_db.return_value = None

    with app.test_client() as client:
        with client.session_transaction() as sess:
            sess['authenticated'] = True

        response = client.get('/detail?name=Test')
        assert response.status_code == 200  # ← не 302!
        # Можно проверить, что рынок не найден
        assert 'Рынок не найден' not in response.get_data(as_text=True)  # потому что name есть, но БД недоступна
        # Но в текущей логике — просто пустой market

@patch('app.app.get_db_connection')
def test_feedback_requires_auth(mock_get_db):
    """Попытка доступа без авторизации → редирект на /login"""
    with app.test_client() as client:
        response = client.get('/feedback', follow_redirects=False)
        assert response.status_code == 302
        assert response.location.endswith('/')


@patch('app.app.get_db_connection')
def test_feedback_get_renders_form(mock_get_db):
    """GET /feedback → отображается форма"""
    with app.test_client() as client:
        with client.session_transaction() as sess:
            sess['authenticated'] = True

        response = client.get('/feedback')
        assert response.status_code == 200
        assert 'Отправить отзыв' in response.get_data(as_text=True)


@patch('app.app.get_db_connection')
def test_feedback_missing_fields(mock_get_db):
    """Не заполнены обязательные поля → ошибка"""
    with app.test_client() as client:
        with client.session_transaction() as sess:
            sess['authenticated'] = True

        response = client.post('/feedback', data={
            'market_name': '',
            'user_name': '',
            'rating': '3'
        })
        assert response.status_code == 200
        assert 'Заполните все обязательные поля' in response.get_data(as_text=True)


@patch('app.app.get_db_connection')
def test_feedback_invalid_rating(mock_get_db):
    """Некорректный рейтинг → ошибка"""
    with app.test_client() as client:
        with client.session_transaction() as sess:
            sess['authenticated'] = True

        for bad_rating in ['0', '6', 'abc', '']:
            response = client.post('/feedback', data={
                'market_name': 'Центральный рынок',
                'user_name': 'Иван',
                'rating': bad_rating
            })
            assert response.status_code == 200
            assert 'Рейтинг должен быть числом от 1 до 5' in response.get_data(as_text=True)


@patch('app.app.get_db_connection')
def test_feedback_market_not_found(mock_get_db):
    """Рынок не найден → ошибка"""
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_get_db.return_value = mock_conn
    mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
    mock_cursor.fetchone.return_value = None  # farmers_markets не нашёл запись

    with app.test_client() as client:
        with client.session_transaction() as sess:
            sess['authenticated'] = True

        response = client.post('/feedback', data={
            'market_name': 'Неизвестный рынок',
            'user_name': 'Иван',
            'rating': '4',
            'review_text': 'Хорошо!'
        })
        assert response.status_code == 200
        assert 'Рынок не найден' in response.get_data(as_text=True)


@patch('app.app.get_db_connection')
def test_feedback_success(mock_get_db):
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_get_db.return_value = mock_conn
    mock_conn.cursor.return_value.__enter__.return_value = mock_cursor

    # Настройка: первый fetchone() вернёт market_id, второй — ничего (для INSERT не используется)
    mock_cursor.fetchone.return_value = {'market_id': 123}

    with app.test_client() as client:
        with client.session_transaction() as sess:
            sess['authenticated'] = True

        response = client.post('/feedback', data={
            'market_name': 'Центральный рынок',
            'user_name': 'Иван',
            'rating': '5',
            'review_text': 'Отличный рынок!'
        })

        # После POST — рендер той же страницы (без редиректа)
        assert response.status_code == 200
        assert '✅ Отзыв успешно добавлен!' in response.get_data(as_text=True)

        # Проверяем, что SELECT был вызван с правильным параметром
        mock_cursor.execute.assert_any_call(
            "SELECT market_id FROM farmers_markets WHERE LOWER(TRIM(market_name)) = %s",
            ("центральный рынок",)
        )

        # Проверяем, что INSERT был вызван
        # Используем ANY для проверки, что вызов был, без жёсткой привязки к порядку
        insert_calls = [
            call for call in mock_cursor.execute.call_args_list
            if call[0][0].strip().startswith("INSERT INTO reviews")
        ]
        assert len(insert_calls) == 1
        args = insert_calls[0][0]  # (query, params)
        assert args[1] == (123, 'Иван', 5, 'Отличный рынок!')

        mock_conn.commit.assert_called_once()

@patch('app.app.get_db_connection')
def test_delete_requires_auth(mock_get_db):
    """Неавторизованный пользователь → редирект на /login"""
    with app.test_client() as client:
        response = client.get('/delete', follow_redirects=False)
        assert response.status_code == 302
        assert response.location.endswith('/')


@patch('app.app.get_db_connection')
def test_delete_requires_admin(mock_get_db):
    """Обычный пользователь (не админ) → редирект на /markets"""
    with app.test_client() as client:
        with client.session_transaction() as sess:
            sess['authenticated'] = True
            sess['is_admin'] = False  # ← не админ

        response = client.get('/delete', follow_redirects=False)
        assert response.status_code == 302
        assert response.location.endswith('/markets')


@patch('app.app.get_db_connection')
def test_delete_get_renders_form(mock_get_db):
    """GET /delete → отображается форма удаления"""
    with app.test_client() as client:
        with client.session_transaction() as sess:
            sess['authenticated'] = True
            sess['is_admin'] = True

        response = client.get('/delete')
        html = response.get_data(as_text=True)
        assert response.status_code == 200
        assert 'name="market_name"' in html
        assert '<form' in html


@patch('app.app.get_db_connection')
def test_delete_empty_name(mock_get_db):
    """Пустое имя рынка → ошибка"""
    with app.test_client() as client:
        with client.session_transaction() as sess:
            sess['authenticated'] = True
            sess['is_admin'] = True

        response = client.post('/delete', data={'market_name': ''})
        assert response.status_code == 200
        assert 'Введите название рынка' in response.get_data(as_text=True)


@patch('app.app.get_db_connection')
def test_delete_market_not_found(mock_get_db):
    """Рынок не найден → сообщение об ошибке"""
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_get_db.return_value = mock_conn
    mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
    mock_cursor.fetchone.return_value = None  # DELETE ... RETURNING не вернул запись

    with app.test_client() as client:
        with client.session_transaction() as sess:
            sess['authenticated'] = True
            sess['is_admin'] = True

        response = client.post('/delete', data={'market_name': 'Неизвестный'})
        assert response.status_code == 200
        assert '❌ Рынок не найден.' in response.get_data(as_text=True)
        mock_conn.commit.assert_not_called()  # транзакция не коммитится


@patch('app.app.get_db_connection')
def test_delete_success(mock_get_db):
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_get_db.return_value = mock_conn
    mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
    mock_cursor.fetchone.return_value = {'market_id': 999}  # эмулируем успешное удаление

    with app.test_client() as client:
        with client.session_transaction() as sess:
            sess['authenticated'] = True
            sess['is_admin'] = True

        response = client.post('/delete', data={'market_name': 'Центральный рынок'})
        html = response.get_data(as_text=True)
        assert response.status_code == 200

        # Проверяем НАЛИЧИЕ ключевых частей сообщения (без жёсткой привязки к кавычкам и пунктуации)
        assert "✅" in html
        assert "удалён" in html
        assert "Центральный рынок" in html

        # Или: проверяем, что flash-сообщение было добавлено (косвенно через контекст)
        # Но проще — довериться логике и проверить SQL
        mock_cursor.execute.assert_any_call(
            "DELETE FROM farmers_markets WHERE LOWER(TRIM(market_name)) = %s RETURNING market_id",
            ("центральный рынок",)
        )
        mock_conn.commit.assert_called_once()

@patch('app.app.get_db_connection')
def test_add_market_requires_auth(mock_get_db):
    """Неавторизованный → редирект на /login"""
    with app.test_client() as client:
        response = client.get('/add_market', follow_redirects=False)
        assert response.status_code == 302
        assert response.location.endswith('/')


@patch('app.app.get_db_connection')
def test_add_market_requires_admin(mock_get_db):
    """Обычный пользователь → редирект на /markets"""
    with app.test_client() as client:
        with client.session_transaction() as sess:
            sess['authenticated'] = True
            sess['is_admin'] = False

        response = client.get('/add_market', follow_redirects=False)
        assert response.status_code == 302
        assert response.location.endswith('/markets')


@patch('app.app.get_db_connection')
def test_add_market_get_renders_form(mock_get_db):
    """GET /add_market → форма с выпадающими списками"""
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_get_db.return_value = mock_conn
    mock_conn.cursor.return_value.__enter__.return_value = mock_cursor

    mock_cursor.fetchall.side_effect = [
        [{'product_id': 1, 'product_name': 'Овощи'}, {'product_id': 2, 'product_name': 'Фрукты'}],
        [{'payment_id': 1, 'payment_name': 'Наличные'}, {'payment_id': 2, 'payment_name': 'Карта'}],
        [{'social_network_id': 1, 'social_networks': 'Instagram'}, {'social_network_id': 2, 'social_networks': 'ВКонтакте'}]
    ]

    with app.test_client() as client:
        with client.session_transaction() as sess:
            sess['authenticated'] = True
            sess['is_admin'] = True

        response = client.get('/add_market')
        html = response.get_data(as_text=True)
        assert response.status_code == 200
        assert 'name="market_name"' in html
        assert 'Овощи' in html
        assert 'Наличные' in html
        assert 'Instagram' in html


@patch('app.app.get_db_connection')
def test_add_market_missing_required_fields(mock_get_db):
    """Отсутствуют обязательные поля → flash ошибка"""
    with app.test_client() as client:
        with client.session_transaction() as sess:
            sess['authenticated'] = True
            sess['is_admin'] = True

        response = client.post('/add_market', data={
            'market_name': '',
            'street': '',
            'city': 'Москва',
            'state': 'Москва',
            'zip': ''  # ← не хватает
        })
        assert response.status_code == 302  # редирект на /add_market
        # Проверим flash через GET после редиректа
        response2 = client.get('/add_market')
        assert 'Все поля кроме координат и описания местоположения обязательны.' in response2.get_data(as_text=True)


@patch('app.app.get_db_connection')
def test_add_market_invalid_coords(mock_get_db):
    """Некорректные координаты → ошибка"""
    with app.test_client() as client:
        with client.session_transaction() as sess:
            sess['authenticated'] = True
            sess['is_admin'] = True

        # Только x указан
        response = client.post('/add_market', data={
            'market_name': 'Тест',
            'street': 'Ленина',
            'city': 'Москва',
            'state': 'Москва',
            'zip': '101000',
            'x': '55.7558'
            # y отсутствует
        })
        assert response.status_code == 302
        response2 = client.get('/add_market')
        assert 'Укажите обе координаты или оставьте обе пустыми.' in response2.get_data(as_text=True)


@patch('app.app.get_db_connection')
def test_add_market_success(mock_get_db):
    """Успешное добавление рынка"""
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_get_db.return_value = mock_conn
    mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
    mock_cursor.fetchone.return_value = {'market_id': 999}

    with app.test_client() as client:
        with client.session_transaction() as sess:
            sess['authenticated'] = True
            sess['is_admin'] = True

        response = client.post('/add_market', data={
            'market_name': 'Новый рынок',
            'street': 'Гагарина, 1',
            'city': 'СПб',
            'state': 'СПб',
            'zip': '190000',
            'x': '59.9343',
            'y': '30.3351',
            'location': 'У метро',
            'products': ['1', '2'],
            'payments': ['1'],
            'social_networks': ['1'],
            'social_urls': ['https://insta.com/new']
        })

        # После успеха — редирект на /markets
        assert response.status_code == 302
        assert response.location.endswith('/markets')

        # Проверяем вызовы SQL
        calls = [call[0] for call in mock_cursor.execute.call_args_list]
        assert any("INSERT INTO farmers_markets" in q for q, _ in calls)
        assert any("INSERT INTO market_products" in q for q, _ in calls)
        assert any("INSERT INTO market_payments" in q for q, _ in calls)
        assert any("INSERT INTO market_social_links" in q for q, _ in calls)

        mock_conn.commit.assert_called_once()

@patch('app.app.get_db_connection')
def test_import_markets_requires_auth(mock_get_db):
    """Неавторизованный → редирект на /login"""
    with app.test_client() as client:
        response = client.get('/import_markets', follow_redirects=False)
        assert response.status_code == 302
        assert response.location.endswith('/')


@patch('app.app.get_db_connection')
def test_import_markets_requires_admin(mock_get_db):
    """Обычный пользователь → редирект на /markets"""
    with app.test_client() as client:
        with client.session_transaction() as sess:
            sess['authenticated'] = True
            sess['is_admin'] = False

        response = client.get('/import_markets', follow_redirects=False)
        assert response.status_code == 302
        assert response.location.endswith('/markets')


@patch('app.app.get_db_connection')
def test_import_markets_get_renders_form(mock_get_db):
    """GET /import_markets → форма загрузки"""
    with app.test_client() as client:
        with client.session_transaction() as sess:
            sess['authenticated'] = True
            sess['is_admin'] = True

        response = client.get('/import_markets')
        assert response.status_code == 200
        assert 'type="file"' in response.get_data(as_text=True)
        assert 'name="excel_file"' in response.get_data(as_text=True)


@patch('app.app.get_db_connection')
def test_import_markets_no_file(mock_get_db):
    """Файл не загружен → ошибка"""
    with app.test_client() as client:
        with client.session_transaction() as sess:
            sess['authenticated'] = True
            sess['is_admin'] = True

        response = client.post('/import_markets', data={})
        assert response.status_code == 302
        response2 = client.get('/import_markets')
        assert 'загрузите файл Excel' in response2.get_data(as_text=True)


@patch('app.app.save_file_to_minio_and_log')  # ← Мокаем ВСЮ функцию
@patch('app.app.get_db_connection')
def test_import_markets_success(mock_get_db, mock_save_file):
    """Успешный импорт Excel-файла"""
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_get_db.return_value = mock_conn
    mock_conn.cursor.return_value.__enter__.return_value = mock_cursor

    # Эмуляция справочников
    mock_cursor.fetchall.side_effect = [
        [{'product_id': 1, 'product_name': 'Овощи'}, {'product_id': 2, 'product_name': 'Фрукты'}],
        [{'payment_id': 1, 'payment_name': 'Наличные'}, {'payment_id': 2, 'payment_name': 'Карта'}],
        [{'social_network_id': 1, 'social_networks': 'Instagram'}, {'social_network_id': 2, 'social_networks': 'ВКонтакте'}]
    ]
    mock_cursor.fetchone.return_value = {'market_id': 999}

    # Создаём Excel
    df = pd.DataFrame({
        'market_name': ['Новый рынок'],
        'street': ['Ленина, 1'],
        'city': ['Москва'],
        'state': ['Москва'],
        'zip': ['101000'],
        'products': ['Овощи, Фрукты'],
        'payments': ['Наличные'],
        'socials': ['Instagram:https://insta.com']
    })
    file_data = io.BytesIO()
    df.to_excel(file_data, index=False)
    file_data.seek(0)

    with app.test_client() as client:
        with client.session_transaction() as sess:
            sess['authenticated'] = True
            sess['is_admin'] = True

        data = {'excel_file': (file_data, 'test.xlsx')}
        response = client.post('/import_markets', data=data, content_type='multipart/form-data')

        assert response.status_code == 302
        assert response.location.endswith('/markets')

        # Проверяем, что save_file_to_minio_and_log был вызван
        mock_save_file.assert_called_once()

        # Проверяем, что основной INSERT выполнен
        insert_calls = [call for call in mock_cursor.execute.call_args_list if 'INSERT INTO farmers_markets' in call[0][0]]
        assert len(insert_calls) == 1

        # Теперь commit вызывается ТОЛЬКО один раз — в основном блоке
        mock_conn.commit.assert_called_once()

@patch('app.app.get_db_connection')
def test_import_markets_missing_columns(mock_get_db):
    """Отсутствуют обязательные колонки → ошибка"""
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_get_db.return_value = mock_conn
    mock_conn.cursor.return_value.__enter__.return_value = mock_cursor

    # Некорректный Excel (нет 'zip')
    df = pd.DataFrame({
        'market_name': ['Тест'],
        'street': ['ул. Пушкина'],
        'city': ['СПб'],
        'state': ['СПб']
        # ← нет 'zip'
    })
    file_data = io.BytesIO()
    df.to_excel(file_data, index=False)
    file_data.seek(0)

    with app.test_client() as client:
        with client.session_transaction() as sess:
            sess['authenticated'] = True
            sess['is_admin'] = True

        data = {
            'excel_file': (file_data, 'bad.xlsx')
        }
        response = client.post('/import_markets', data=data, content_type='multipart/form-data')
        assert response.status_code == 302
        response2 = client.get('/import_markets')
        assert 'обязательные колонки' in response2.get_data(as_text=True)

def test_download_template_requires_auth():
    """Неавторизованный пользователь → редирект на /login"""
    with app.test_client() as client:
        response = client.get('/download_template', follow_redirects=False)
        assert response.status_code == 302
        assert response.location.endswith('/')


def test_download_template_success():
    """Успешная генерация и скачивание шаблона Excel"""
    with app.test_client() as client:
        with client.session_transaction() as sess:
            sess['authenticated'] = True

        response = client.get('/download_template')
        assert response.status_code == 200
        assert response.headers['Content-Type'] == 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'

        # Проверяем, что Content-Disposition содержит .xlsx (имя может быть закодировано)
        content_disp = response.headers.get('Content-Disposition', '')
        assert 'filename=' in content_disp
        assert '.xlsx' in content_disp

        # Проверяем содержимое Excel
        excel_data = BytesIO(response.data)
        df = pd.read_excel(excel_data)

        required_cols = {'market_name', 'street', 'city', 'state', 'zip', 'x', 'y', 'location', 'products', 'payments', 'socials'}
        assert required_cols.issubset(set(df.columns)), f"Отсутствуют колонки: {required_cols - set(df.columns)}"
        assert len(df) >= 2
        assert 'Центральный рынок' in df['market_name'].values

@patch('app.app.get_db_connection')
def test_edit_market_requires_auth(mock_get_db):
    """Неавторизованный → редирект на /login"""
    with app.test_client() as client:
        response = client.get('/edit_market', follow_redirects=False)
        assert response.status_code == 302
        assert response.location.endswith('/')


@patch('app.app.get_db_connection')
def test_edit_market_requires_admin(mock_get_db):
    """Обычный пользователь → редирект на /markets"""
    with app.test_client() as client:
        with client.session_transaction() as sess:
            sess['authenticated'] = True
            sess['is_admin'] = False

        response = client.get('/edit_market', follow_redirects=False)
        assert response.status_code == 302
        assert response.location.endswith('/markets')


@patch('app.app.get_db_connection')
def test_edit_market_get_not_found(mock_get_db):
    """Рынок не найден → flash ошибка"""
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_get_db.return_value = mock_conn
    mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
    mock_cursor.fetchone.return_value = None  # farmers_markets не нашёл запись

    with app.test_client() as client:
        with client.session_transaction() as sess:
            sess['authenticated'] = True
            sess['is_admin'] = True

        response = client.get('/edit_market?name=Неизвестный')
        assert response.status_code == 200
        assert 'Рынок не найден' in response.get_data(as_text=True)


@patch('app.app.get_db_connection')
def test_edit_market_get_success(mock_get_db):
    """Успешная загрузка формы редактирования"""
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_get_db.return_value = mock_conn
    mock_conn.cursor.return_value.__enter__.return_value = mock_cursor

    # Эмуляция данных
    market_row = {
        'market_id': 123,
        'market_name': 'Центральный рынок',
        'street': 'Ленина, 1',
        'city': 'Москва',
        'state': 'Москва',
        'zip': '101000',
        'x': 55.7558,
        'y': 37.6176,
        'location': 'У фонтана'
    }

    def execute_side_effect(query, params=None):
        if 'farmers_markets' in query and 'WHERE' in query:
            mock_cursor.fetchone.return_value = market_row
        elif 'market_products' in query:
            mock_cursor.fetchall.return_value = [{'product_id': 1}]
        elif 'market_payments' in query:
            mock_cursor.fetchall.return_value = [{'payment_id': 2}]
        elif 'market_social_links' in query:
            mock_cursor.fetchall.return_value = [{'social_network_id': 1, 'url': 'https://insta.com'}]
        elif 'products' in query and 'ORDER BY' in query:
            mock_cursor.fetchall.return_value = [{'product_id': 1, 'product_name': 'Овощи'}]
        elif 'payment_methods' in query:
            mock_cursor.fetchall.return_value = [{'payment_id': 2, 'payment_name': 'Карта'}]
        elif 'social_networks' in query:
            mock_cursor.fetchall.return_value = [{'social_network_id': 1, 'social_networks': 'Instagram'}]
        else:
            mock_cursor.fetchone.return_value = None
            mock_cursor.fetchall.return_value = []

    mock_cursor.execute.side_effect = execute_side_effect

    with app.test_client() as client:
        with client.session_transaction() as sess:
            sess['authenticated'] = True
            sess['is_admin'] = True

        response = client.get('/edit_market?name=Центральный+рынок')
        html = response.get_data(as_text=True)
        assert response.status_code == 200
        assert 'Центральный рынок' in html
        assert 'Ленина, 1' in html
        assert 'Овощи' in html
        assert 'Карта' in html
        assert 'Instagram' in html


@patch('app.app.get_db_connection')
def test_edit_market_missing_address_fields(mock_get_db):
    """Отсутствуют обязательные поля адреса → ошибка"""
    with app.test_client() as client:
        with client.session_transaction() as sess:
            sess['authenticated'] = True
            sess['is_admin'] = True

        response = client.post('/edit_market', data={
            'market_id': '123',
            'original_name': 'Старое имя',
            'street': '',
            'city': 'Москва',
            'state': 'Москва',
            'zip': '101000'
        })
        assert response.status_code == 302
        response2 = client.get('/edit_market?name=Старое+имя')
        assert 'Поля адреса (улица, город, субъект, индекс) обязательны.' in response2.get_data(as_text=True)


@patch('app.app.get_db_connection')
def test_edit_market_success(mock_get_db):
    """Успешное обновление рынка"""
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_get_db.return_value = mock_conn
    mock_conn.cursor.return_value.__enter__.return_value = mock_cursor

    with app.test_client() as client:
        with client.session_transaction() as sess:
            sess['authenticated'] = True
            sess['is_admin'] = True

        response = client.post('/edit_market', data={
            'market_id': '123',
            'original_name': 'Старое имя',
            'street': 'Новая улица, 5',
            'city': 'СПб',
            'state': 'СПб',
            'zip': '190000',
            'x': '59.9343',
            'y': '30.3351',
            'location': 'У метро',
            'products': ['1'],
            'payments': ['2'],
            'social_networks': ['1'],
            'social_urls': ['https://newinsta.com']
        })

        assert response.status_code == 302
        assert response.location.endswith('/markets')

        # Проверяем вызовы SQL
        calls = [call[0][0] for call in mock_cursor.execute.call_args_list]
        assert any("UPDATE farmers_markets" in q for q in calls)
        assert any("DELETE FROM market_products" in q for q in calls)
        assert any("INSERT INTO market_products" in q for q in calls)
        assert any("DELETE FROM market_social_links" in q for q in calls)
        assert any("INSERT INTO market_social_links" in q for q in calls)

        mock_conn.commit.assert_called_once()

@patch('app.app.get_db_connection')
def test_download_pdf_requires_auth(mock_get_db):
    """Неавторизованный → редирект на /login"""
    with app.test_client() as client:
        response = client.get('/download_pdf?name=Test', follow_redirects=False)
        assert response.status_code == 302
        assert response.location.endswith('/')


@patch('app.app.get_db_connection')
def test_download_pdf_no_name(mock_get_db):
    """Не указано название рынка → flash ошибка"""
    with app.test_client() as client:
        with client.session_transaction() as sess:
            sess['authenticated'] = True

        response = client.get('/download_pdf')
        assert response.status_code == 302
        response2 = client.get('/detail')
        assert 'Не указано название рынка' in response2.get_data(as_text=True)


@patch('app.app.get_db_connection')
def test_download_pdf_market_not_found(mock_get_db):
    """Рынок не найден → flash ошибка"""
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_get_db.return_value = mock_conn
    mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
    mock_cursor.fetchone.return_value = None

    with app.test_client() as client:
        with client.session_transaction() as sess:
            sess['authenticated'] = True

        response = client.get('/download_pdf?name=Неизвестный')
        assert response.status_code == 302
        response2 = client.get('/detail?name=Неизвестный')
        assert 'Рынок не найден' in response2.get_data(as_text=True)


@patch('app.app.save_file_to_minio_and_log')
@patch('app.app.get_db_connection')
def test_download_pdf_success(mock_get_db, mock_save_file):
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_get_db.return_value = mock_conn
    mock_conn.cursor.return_value.__enter__.return_value = mock_cursor

    # Эмуляция данных
    market_row = {
        'market_id': 123,
        'market_name': 'Центральный рынок',
        'street': 'Ленина, 1',
        'city': 'Москва',
        'state': 'Москва',
        'zip': '101000',
        'x': 55.7558,
        'y': 37.6176,
        'location': 'У фонтана'
    }

    def execute_side_effect(query, params=None):
        if 'farmers_markets' in query and 'WHERE' in query:
            mock_cursor.fetchone.return_value = market_row
        elif 'market_products' in query:
            mock_cursor.fetchall.return_value = [{'product_name': 'Овощи'}]
        elif 'market_payments' in query:
            mock_cursor.fetchall.return_value = [{'payment_name': 'Наличные'}]
        elif 'market_social_links' in query:
            mock_cursor.fetchall.return_value = [{'social_networks': 'Instagram', 'url': 'https://insta.com'}]
        elif 'reviews' in query:
            from datetime import datetime
            mock_cursor.fetchall.return_value = [{
                'user_name': 'Иван',
                'rating': 5,
                'review_text': 'Отлично!',
                'created_at': datetime(2025, 1, 15)
            }]
        else:
            mock_cursor.fetchone.return_value = None
            mock_cursor.fetchall.return_value = []

    mock_cursor.execute.side_effect = execute_side_effect

    with app.test_client() as client:
        with client.session_transaction() as sess:
            sess['authenticated'] = True

        response = client.get('/download_pdf?name=Центральный+рынок')
        assert response.status_code == 200
        assert response.headers['Content-Type'] == 'application/pdf'

        # Проверяем, что Content-Disposition содержит .pdf (имя может быть закодировано)
        content_disp = response.headers.get('Content-Disposition', '')
        assert 'filename=' in content_disp
        assert '.pdf' in content_disp

        # Проверяем, что save_file_to_minio_and_log был вызван
        mock_save_file.assert_called_once()

        # Проверяем, что PDF не пустой
        assert len(response.data) > 1000

@patch('app.app.save_file_to_minio_and_log')
@patch('app.app.create_engine')
def test_export_all_success(mock_create_engine, mock_save_file):
    mock_conn = MagicMock()
    mock_result = MagicMock()
    mock_engine = MagicMock()
    mock_create_engine.return_value = mock_engine
    mock_engine.connect.return_value.__enter__.return_value = mock_conn

    mock_result.keys.return_value = ['market_name', 'city', 'state', 'zip']
    mock_result.fetchmany.side_effect = [
        [('Центральный рынок', 'Москва', 'Москва', '101000')],
        []
    ]
    mock_conn.execution_options.return_value.execute.return_value = mock_result

    with app.test_client() as client:
        with client.session_transaction() as sess:
            sess['authenticated'] = True

        response = client.get('/export_all')
        assert response.status_code == 200
        assert response.headers['Content-Type'] == 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'

        # Проверяем, что имя файла содержит .xlsx (имя может быть закодировано)
        content_disp = response.headers.get('Content-Disposition', '')
        assert '.xlsx' in content_disp

        # Проверяем Excel
        excel_data = io.BytesIO(response.data)
        df = pd.read_excel(excel_data)
        assert list(df.columns) == ['market_name', 'city', 'state', 'zip']
        assert len(df) == 1

        mock_save_file.assert_called_once()


@patch('app.app.create_engine')
def test_export_all_db_error(mock_create_engine):
    mock_create_engine.side_effect = Exception("Connection failed")

    with app.test_client() as client:
        with client.session_transaction() as sess:
            sess['authenticated'] = True

        response = client.get('/export_all')
        assert response.status_code == 302
        assert response.location.endswith('/markets')

@patch('app.app.get_db_connection')
def test_stats_requires_auth(mock_get_db):
    """Неавторизованный → редирект на /login"""
    with app.test_client() as client:
        response = client.get('/stats', follow_redirects=False)
        assert response.status_code == 302
        assert response.location.endswith('/')


@patch('app.app.get_db_connection')
def test_stats_db_error(mock_get_db):
    """Ошибка подключения к БД → flash и редирект на /login"""
    mock_get_db.return_value = None

    with app.test_client() as client:
        with client.session_transaction() as sess:
            sess['authenticated'] = True

        response = client.get('/stats')
        assert response.status_code == 302
        assert response.location.endswith('/')


@patch('app.app.get_db_connection')
def test_stats_success(mock_get_db):
    """Успешная загрузка статистики"""
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_get_db.return_value = mock_conn
    mock_conn.cursor.return_value.__enter__.return_value = mock_cursor

    # Эмуляция последовательных fetchone/fetchall
    def execute_side_effect(query, params=None):
        if 'COUNT(*) AS total FROM farmers_markets' in query:
            mock_cursor.fetchone.return_value = {'total': 150}
        elif 'COUNT(*) AS total FROM reviews' in query:
            mock_cursor.fetchone.return_value = {'total': 300}
        elif 'COUNT(*) AS total FROM products' in query:
            mock_cursor.fetchone.return_value = {'total': 20}
        elif 'COUNT(*) AS total FROM payment_methods' in query:
            mock_cursor.fetchone.return_value = {'total': 5}
        elif 'COUNT(*) AS total FROM social_networks' in query:
            mock_cursor.fetchone.return_value = {'total': 3}
        elif 'COALESCE(ROUND(AVG(rating), 2), 0) AS avg_rating' in query:
            mock_cursor.fetchone.return_value = {'avg_rating': 4.25}
        elif 'Топ-5 рынков' in query or 'HAVING COUNT(r.review_id) > 0' in query:
            mock_cursor.fetchall.return_value = [
                {'market_name': 'Центральный рынок', 'city': 'Москва', 'state': 'Москва', 'avg_rating': 4.8, 'review_count': 50},
                {'market_name': 'Зелёный базар', 'city': 'СПб', 'state': 'СПб', 'avg_rating': 4.7, 'review_count': 45}
            ]
        elif 'Рынки по субъектам' in query or 'GROUP BY state' in query:
            mock_cursor.fetchall.return_value = [
                {'state': 'Москва', 'count': 40},
                {'state': 'СПб', 'count': 30}
            ]
        else:
            mock_cursor.fetchone.return_value = {'total': 0}
            mock_cursor.fetchall.return_value = []

    mock_cursor.execute.side_effect = execute_side_effect

    with app.test_client() as client:
        with client.session_transaction() as sess:
            sess['authenticated'] = True

        response = client.get('/stats')
        html = response.get_data(as_text=True)
        assert response.status_code == 200
        assert 'Статистика' in html
        assert '150' in html  # total_markets
        assert '300' in html  # total_reviews
        assert '4.25' in html  # avg_rating
        assert 'Центральный рынок' in html
        assert 'Москва' in html and '40' in html  # markets_by_state