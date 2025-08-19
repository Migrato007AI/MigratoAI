
CREATE SCHEMA IF NOT EXISTS migration_app;



CREATE TABLE IF NOT EXISTS migration_app.companyinfo (
    companyid SERIAL PRIMARY KEY,
    companyname VARCHAR(255) NOT NULL
);


CREATE TABLE IF NOT EXISTS migration_app.teaminfo (
    teamid SERIAL PRIMARY KEY,
    teamname VARCHAR(255) NOT NULL
);



CREATE TABLE IF NOT EXISTS migration_app."user" (
    userid SERIAL PRIMARY KEY,
    username VARCHAR(255) NOT NULL
);


CREATE TABLE IF NOT EXISTS migration_app.role (
    roleid SERIAL PRIMARY KEY,
    rolename VARCHAR(255) NOT NULL
);


DROP TABLE IF EXISTS migration_app.resource;


CREATE TABLE IF NOT EXISTS migration_app.resource (
    resourceid SERIAL PRIMARY KEY,
    resourcename VARCHAR(255) NOT NULL,
    resourcetype VARCHAR(255) NOT NULL
);


DROP TABLE IF EXISTS migration_app.userroleresource;

CREATE TABLE IF NOT EXISTS migration_app.userroleresource (
    userroleresourceid SERIAL PRIMARY KEY,
    userid INT REFERENCES migration_app."user"(userid),
    roleid INT REFERENCES migration_app.role(roleid),
    resourceid INT REFERENCES migration_app.resource(resourceid)
);


Drop TABLE IF EXISTS migration_app.projectinfo;

CREATE TABLE IF NOT EXISTS migration_app.projectinfo (
    projectid SERIAL PRIMARY KEY,
    projectname VARCHAR(255) NOT NULL,
    projectdescription TEXT,
    blob_storage_name TEXT,
    blob_container_name TEXT,
    blob_account_key TEXT
);


DROP TABLE IF EXISTS migration_app.admininfo;

CREATE TABLE IF NOT EXISTS migration_app.admininfo (
    adminid SERIAL PRIMARY KEY,
    username VARCHAR(255) NOT NULL,
    password VARCHAR(255) NOT NULL,
    officialemail VARCHAR(255) NOT NULL,
    companyid INT REFERENCES migration_app.companyinfo(companyid),
    teamid INT REFERENCES migration_app.teaminfo(teamid),
    projectid INT REFERENCES migration_app.projectinfo(projectid)
);

DROP TABLE IF EXISTS migration_app.projectResource;

CREATE TABLE IF NOT EXISTS migration_app.projectResource (
    projectResourceid SERIAL PRIMARY KEY,
    projectid INT REFERENCES migration_app.projectinfo(projectid),
    resourceid INT REFERENCES migration_app.resource(resourceid),
    GIT_commit_required BOOLEAN DEFAULT FALSE,
    Organization_name VARCHAR(255),
    reponame VARCHAR(255)
);

DROP TABLE IF EXISTS migration_app.UserProject;
CREATE TABLE IF NOT EXISTS migration_app.UserProject (
    userProjectid SERIAL PRIMARY KEY,
    userid INT REFERENCES migration_app."user"(userid),
    projectid INT REFERENCES migration_app.projectinfo(projectid)
);



DELETE from  migration_app.UserProject;
DELETE from  migration_app.projectResource;
DELETE from migration_app.admininfo;
DELETE from migration_app.projectinfo;
DELETE from migration_app.userroleresource;
DELETE from migration_app."user";
DELETE FROM migration_app.companyinfo;
DELETE FROM migration_app.teaminfo;



CREATE OR REPLACE FUNCTION migration_app.get_userproject_mapping(username_in VARCHAR(255))
RETURNS TABLE (
    username VARCHAR(255),
    projectname VARCHAR(255)
)
LANGUAGE plpgsql
AS $function$
BEGIN
    RETURN QUERY
    SELECT U.username, P.projectname
    FROM migration_app."user" U
    INNER JOIN migration_app.UserProject UP ON U.userid = UP.userid
    INNER JOIN migration_app.projectinfo P ON UP.projectid = P.projectid
    WHERE U.USERNAME = username_in;
END;
$function$;



CREATE OR REPLACE FUNCTION migration_app.get_project_info(username_in VARCHAR(255), projectname_in VARCHAR(255))
RETURNS TABLE (
    projectname VARCHAR(255),
    username VARCHAR(255),
    rolename VARCHAR(255),
    resourcename VARCHAR(255),
    resourcetype VARCHAR(255),
    enable_git_commit BOOLEAN,
    organization_name VARCHAR(255),
    reponame VARCHAR(255)
)
LANGUAGE plpgsql
AS $function$

BEGIN
    RETURN QUERY
    SELECT P.projectname, U.username, R.rolename, R.resourcename, R.resourcetype,
           PR.enable_git_commit, PR.organization_name, PR.reponame
    FROM migration_app.projectinfo P
    INNER JOIN migration_app.UserProject UP ON P.projectid = UP.projectid
    INNER JOIN migration_app."user" U ON UP.userid = U.userid
    INNER JOIN migration_app.userroleresource UR ON U.userid = UR.userid
    INNER JOIN migration_app.role R ON UR.roleid = R.roleid
    INNER JOIN migration_app.resource R ON UR.resourceid = R.resourceid 
    INNER JOIN migration_app.projectResource PR on P.projectid = PR.projectid and R.resourceid = PR.resourceid
    WHERE U.username = username_in and P.Projectname = projectname_in;
END;

$function$;





CREATE OR REPLACE PROCEDURE migration_app.sp_insert_admininfo(
    IN username_in VARCHAR(255),
    IN password_in VARCHAR(255),
    IN officialemail_in VARCHAR(255),
    IN companyname_in VARCHAR(255),
    IN teamname_in VARCHAR(255),
    IN projectname_in VARCHAR(255),
    IN projectdescription_in TEXT,
    IN dbresources_in TEXT,
    IN etlresources_in TEXT,
    IN blob_storage_name_in TEXT,
    IN blob_container_name_in TEXT,
    IN blob_account_key_in TEXT
)
LANGUAGE plpgsql
AS $procedure$
DECLARE
    v_companyid INT;
    v_teamid INT;
    v_projectid INT;
    v_userid INT;
BEGIN
    -- Get company, team, and project IDs
    INSERT INTO migration_app.companyinfo (companyname)
    SELECT companyname_in
    WHERE NOT EXISTS (SELECT 1 FROM migration_app.companyinfo WHERE companyname = companyname_in);

    INSERT INTO migration_app.teaminfo (teamname)
    SELECT teamname_in
    WHERE NOT EXISTS (SELECT 1 FROM migration_app.teaminfo WHERE teamname = teamname_in);



    SELECT companyid INTO v_companyid FROM migration_app.companyinfo WHERE companyname = companyname_in;
    SELECT teamid INTO v_teamid FROM migration_app.teaminfo WHERE teamname = teamname_in;
    

    -- Insert user if not exists, then get userid
    INSERT INTO migration_app."user" (username)
    SELECT username_in
    WHERE NOT EXISTS (SELECT 1 FROM migration_app."user" WHERE username = username_in);
    SELECT userid INTO v_userid FROM migration_app."user" WHERE username = username_in;


    INSERT INTO migration_app.projectinfo (
    projectname, projectdescription, blob_storage_name, blob_container_name, blob_account_key
    ) 
    SELECT  projectname_in, projectdescription_in, blob_storage_name_in, blob_container_name_in, blob_account_key_in
    WHERE NOT EXISTS (SELECT 1 FROM migration_app.projectinfo WHERE projectname = projectname_in);


    SELECT projectid INTO v_projectid FROM migration_app.projectinfo WHERE projectname = projectname_in;
    
    INSERT INTO migration_app.UserProject (userid, projectid)
    SELECT v_userid, v_projectid
    WHERE NOT EXISTS (SELECT 1 FROM migration_app.UserProject WHERE userid = v_userid AND projectid = v_projectid);

    -- Insert admininfo
    INSERT INTO migration_app.admininfo (username, password, officialemail, companyid, teamid, projectid)
    SELECT username_in, password_in, officialemail_in, v_companyid, v_teamid, v_projectid 
    WHERE NOT EXISTS (SELECT 1 FROM migration_app.admininfo WHERE username = username_in AND projectid = v_projectid);

    


    CREATE TEMP TABLE temp_resources AS
        SELECT resourceid FROM migration_app.resource WHERE resourcename = ANY(string_to_array(etlresources_in, ','))
        UNION
        SELECT resourceid FROM migration_app.resource WHERE resourcename = ANY(string_to_array(dbresources_in, ','));

    -- Insert into userroleresource for each resource
    INSERT INTO migration_app.userroleresource(userid, roleid, resourceid)
    SELECT v_userid, r.roleid, tr.resourceid
    FROM temp_resources tr
    CROSS JOIN (SELECT roleid FROM migration_app.role WHERE rolename = 'admin') r;

    DROP TABLE IF EXISTS temp_resources;
END;
$procedure$;


CREATE OR REPLACE PROCEDURE migration_app.sp_insert_user(
    IN username_in VARCHAR(255),
    IN projectname_in VARCHAR(255),
    IN role_in VARCHAR(255),
    IN resources_in TEXT
)
LANGUAGE plpgsql
AS $procedure$

DECLARE v_projectid INT;
       v_roleid INT;
       v_resourceid INT;
       v_userid INT;

BEGIN

    INSERT INTO migration_app."user" (username)
    SELECT username_in
    WHERE NOT EXISTS (SELECT 1 FROM migration_app."user" WHERE username = username_in);

    SELECT userid INTO v_userid FROM migration_app."user" WHERE username = username_in;

    SELECT projectid INTO v_projectid FROM migration_app.projectinfo WHERE projectname = projectname_in;

    SELECT roleid INTO v_roleid FROM migration_app.role WHERE rolename = role_in;

    SELECT resourceid INTO v_resourceid FROM migration_app.resource WHERE resourcename = ANY(string_to_array(resources_in, ','));

    INSERT INTO migration_app.UserProject (userid, projectid)
    SELECT v_userid, v_projectid
    WHERE NOT EXISTS (SELECT 1 FROM migration_app.UserProject WHERE userid = v_userid AND projectid = v_projectid);

    INSERT INTO migration_app.userroleresource (userid, roleid, resourceid)
    SELECT v_userid, v_roleid, v_resourceid
    WHERE NOT EXISTS (SELECT 1 FROM migration_app.userroleresource WHERE userid = v_userid AND roleid = v_roleid AND resourceid = v_resourceid);

END;
$procedure$;


SELECT * FROM migration_app."user";

SELECT * from migration_app.companyinfo;

SELECT * from migration_app.teaminfo;

SELECT * from  migration_app.admininfo;

