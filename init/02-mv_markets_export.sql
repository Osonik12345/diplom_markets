-- Materialized view for export
DROP MATERIALIZED VIEW IF EXISTS mv_markets_export;

CREATE MATERIALIZED VIEW mv_markets_export AS
SELECT
    fm.market_name,
    fm.street,
    fm.city,
    fm.state,
    fm.zip,
    fm.x,
    fm.y,
    COALESCE(fm.location, '') AS location,
    COALESCE(STRING_AGG(DISTINCT p.product_name, ', ' ORDER BY p.product_name), '') AS products,
    COALESCE(STRING_AGG(DISTINCT pm.payment_name, ', ' ORDER BY pm.payment_name), '') AS payments,
    COALESCE(STRING_AGG(
        DISTINCT sn.social_networks || ':' || COALESCE(msl.url, ''),
        ', ' ORDER BY sn.social_networks || ':' || COALESCE(msl.url, '')
    ), '') AS socials
FROM farmers_markets fm
LEFT JOIN market_products mp ON fm.market_id = mp.market_id
LEFT JOIN products p ON mp.product_id = p.product_id
LEFT JOIN market_payments mpy ON fm.market_id = mpy.market_id
LEFT JOIN payment_methods pm ON mpy.payment_id = pm.payment_id
LEFT JOIN market_social_links msl ON fm.market_id = msl.market_id
LEFT JOIN social_networks sn ON msl.social_network_id = sn.social_network_id
GROUP BY fm.market_id, fm.market_name, fm.street, fm.city, fm.state, fm.zip, fm.x, fm.y, fm.location
ORDER BY fm.market_name;

CREATE INDEX idx_mv_markets_export_name ON mv_markets_export (market_name);

