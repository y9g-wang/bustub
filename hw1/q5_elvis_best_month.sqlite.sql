select artist.name                  as artist_name,
       min(release_info.date_month) as release_month,
       count(*)                     as num_releases
from   artist,
       artist_type,
       artist_credit,
       artist_credit_name,
       release,
       release_info,
       (select artist_name,
               max(num_releases) max_num_release
        from   (select artist.name             as artist_name,
                       release_info.date_month as release_month,
                       count(*)                as num_releases
                from   artist,
                       artist_type,
                       artist_credit,
                       artist_credit_name,
                       release,
                       release_info
                where  artist.name like 'Elvis%'
                  and artist.type = artist_type.id
                  and artist_type.name = 'Person'
                  and artist_credit_name.artist = artist.id
                  and artist_credit_name.artist_credit = artist_credit.id
                  and release_info.date_month is not null
                  and release.artist_credit = artist_credit.id
                  and release.id = release_info.release
                group  by artist.name,
                          release_info.date_month) t1
        group  by artist_name) t2
where  artist.name like 'Elvis%'
  and artist.type = artist_type.id
  and artist_type.name = 'Person'
  and artist_credit_name.artist = artist.id
  and artist_credit_name.artist_credit = artist_credit.id
  and release_info.date_month is not null
  and release.artist_credit = artist_credit.id
  and release.id = release_info.release
  and artist.name = t2.artist_name
group  by artist.name,
          release_info.date_month,
          t2.max_num_release
having count(*) = t2.max_num_release;
-- Run Time: real 28.435 user 19.578420 sys 6.176465
