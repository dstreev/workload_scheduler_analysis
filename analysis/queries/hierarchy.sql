USE ${DB};

CREATE TABLE PATHENUM
(
    PATHLENGTH INT,
    START_     STRING,
    END_       STRING
);

TRUNCATE TABLE PATHENUM;

INSERT INTO
    PATHENUM (PATHLENGTH, START_, END_)
SELECT
    1,
    H1.PARENT,
    H2.PARENT
FROM
    HIERARCHY H1,
    HIERARCHY H2
WHERE
    H1.PARENT = H2.CHILD;

-- Fill in the path levels.
-- ***********
-- RERUN until no new rows are added.
-- ***********
INSERT INTO
    PATHENUM (PATHLENGTH, START_, END_)
SELECT DISTINCT
    (P1.PATHLENGTH + 1),
    P1.START_,
    P2.END_
FROM
    PATHENUM P1,
    PATHENUM P2
WHERE
      P1.END_ = P2.START_
  AND p1.PATHLENGTH = (SELECT MAX(PATHLENGTH) FROM PATHENUM);


SELECT *
FROM
    PATHENUM;

-- All Leaf Nodes
SELECT *
FROM
    HIERARCHY H1
WHERE
    NOT EXISTS (SELECT * FROM HIERARCHY H2 WHERE H1.PARENT = H2.CHILD)

-- Find Levels in Tree
SELECT END_, PATHLENGTH as level from PATHENUM where start_ = "root";



SELECT
    PARENT,
    CHILD,
FROM
    HIERARCHY AS H1,
    HIERARCHY AS H2

-- Attempt to get Hierarchical Parents of a Leaf Queue.
-- Doesn't currently work.  Hive MISSING functional 'RECURSIVE' directive
-- in the 'WITH' clause.
-- Another Option is here: https://blog.pythian.com/recursion-in-hive/
-- Not yet explored. (2020-10-21)
WITH
--     RECURSIVE -- NOT CURRENTLY SUPPORTED IN HIVE
-- Article Reference: https://www.sql-workbench.eu/comparison/recursive_queries.html
HIERARCHY_TREE AS (
    SELECT
        PARENT,
        CHILD,
        CAPACITY
    FROM
        HIERARCHY
    WHERE
            CHILD =
            ${LEAF_QUEUE}
    UNION ALL
    SELECT
        C.PARENT,
        C.CHILD,
        C.CAPACITY
    FROM
        HIERARCHY CHILD
            JOIN HIERARCHY_TREE PARENT
                 ON PARENT.PARENT = CHILD.PARENT
)
SELECT *
FROM
    HIERARCHY_TREE;


WITH
    RECURSIVE cat_tree AS (
   SELECT id,
          name,
          parent_category
   FROM category
   WHERE name = 'Database Software'  -- this defines the start of the recursion
   UNION ALL
   SELECT child.id,
          child.name,
          child.parent_category
   FROM category AS child
     JOIN cat_tree AS parent ON parent.id = child.parent_category -- the self join to the CTE builds up the recursion
)
SELECT *
FROM cat_tree;