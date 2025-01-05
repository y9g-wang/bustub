select work_type.name     as WORK_TYPE,
       w1.name            as WORK_NAME,
       length(w1.comment) as COMMENT_LENGTH,
       w1.comment         as COMMENT
from   work w1,
       work_type,
       (select max(length(work.comment)) as max_work_comment_length,
               work.type                 as work_type
        from   work
        group  by work.type) as w2
where  work_type.id = w1.type
  and length(w1.comment) > 0
  and w1.type = w2.work_type
  and length(w1.comment) = w2.max_work_comment_length
order  by w1.type,
          work_type.name;
-- Run Time (s): real 0.033 user 0.197551 sys 0.029998