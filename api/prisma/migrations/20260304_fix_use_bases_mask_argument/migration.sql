UPDATE "argument"
SET
    "name"        = '--use-bases-mask',
    "value_type"  = 'STRING',
    "is_flag"     = FALSE,
    "description" = 'Override the bases mask string for the run'
WHERE "name" = '--uses-bases-mask';
