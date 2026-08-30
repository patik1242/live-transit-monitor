/* ROW LEVEL SECURITY*/

CREATE OR REPLACE FUNCTION workspace.live_transit.route_filter(route_id INT)
RETURN
    CASE
        WHEN current_user() LIKE '%@softserve.academy'
             OR is_account_group_member('admins')
            THEN TRUE
        WHEN current_user() LIKE '%@gmail.com'
             AND route_id IN (1, 2, 3)
            THEN TRUE
        ELSE FALSE
    END;


ALTER TABLE workspace.live_transit_monitor.gold_route_summary
SET ROW FILTER workspace.live_transit.route_filter ON (routeId);

/*COLUMN LEVEL SECURITY*/
/* Vehicle ID */

CREATE OR REPLACE FUNCTION workspace.live_transit.mask_vehicle_id(vehicle_id INT)
RETURN
    CASE
        WHEN current_user() LIKE '%@softserve.academy'
             OR is_account_group_member('admins')
            THEN vehicle_id
        WHEN current_user() LIKE '%@gmail.com'
            THEN -1
        ELSE NULL
    END;

ALTER TABLE workspace.live_transit_monitor.gold_fleet_current
ALTER COLUMN vehicleId
SET MASK workspace.live_transit.mask_vehicle_id;

/* Vehicle Code */
CREATE OR REPLACE FUNCTION workspace.live_transit.mask_vehicle_code(vehicle_code STRING)
RETURN
    CASE
        WHEN current_user() LIKE '%@softserve.academy'
             OR is_account_group_member('admins')
            THEN vehicle_code
        WHEN current_user() LIKE '%@gmail.com'
            THEN 'MASKED'
        ELSE NULL
    END;

ALTER TABLE workspace.live_transit_monitor.gold_fleet_current
ALTER COLUMN vehicleCode
SET MASK workspace.live_transit.mask_vehicle_code;

/* Latitude */
CREATE OR REPLACE FUNCTION workspace.live_transit.mask_lat(lat FLOAT)
RETURN
    CASE
        WHEN current_user() LIKE '%@softserve.academy'
             OR is_account_group_member('admins')
            THEN lat
        WHEN current_user() LIKE '%@gmail.com'
            THEN ROUND(lat, 2)
        ELSE NULL
    END;

ALTER TABLE workspace.live_transit_monitor.gold_fleet_current
ALTER COLUMN lat
SET MASK workspace.live_transit.mask_lat;

/* Longitude */
CREATE OR REPLACE FUNCTION workspace.live_transit.mask_lon(lon FLOAT)
RETURN
    CASE
        WHEN current_user() LIKE '%@softserve.academy'
             OR is_account_group_member('admins')
            THEN lon
        WHEN current_user() LIKE '%@gmail.com'
            THEN ROUND(lon, 2)
        ELSE NULL
    END;


ALTER TABLE workspace.live_transit_monitor.gold_fleet_current
ALTER COLUMN lon
SET MASK workspace.live_transit.mask_lon;