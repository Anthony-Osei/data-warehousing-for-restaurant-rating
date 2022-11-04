select r.date, b.business_id, (t.min_temp + t.max_temp)/2 as avg_temp,
       p.precipitation, r.stars
       
from review as r
join business as b
on r.biz_id = b.business_id
join temperature as t 
on r.date = t.date
join precipitation as p
on r.date = p.date;