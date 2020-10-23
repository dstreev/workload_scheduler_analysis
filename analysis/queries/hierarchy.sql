USE ${DB};


SELECT
PARENT, CHILD,

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