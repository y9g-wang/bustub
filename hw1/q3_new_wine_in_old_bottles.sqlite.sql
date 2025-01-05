select release.name           as RELEASE_NAME,
       artist.name            as ARTIST_NAME,
       release_info.date_year as RELEASE_YEAR
from   release,
       release_info,
       medium,
       medium_format,
       artist_credit_name,
       artist
where  medium.release = release.id
  and release.id = release_info.release
  and medium_format.id = medium.format
  and medium_format.name = 'Cassette'
  and artist_credit_name.artist_credit = release.artist_credit
  and artist.id = artist_credit_name.artist
order  by release_info.date_year desc,
          release_info.date_month desc,
          release_info.date_day desc,
          release.name
limit 10;
-- Run Time: real 4.206 user 3.996906 sys 0.193555
