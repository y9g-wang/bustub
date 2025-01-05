select release.name                as RELEASE_NAME,
       min(release_info.date_year) as RELEASE_YEAR
from   artist,
       artist_credit_name,
       area,
       release_info,
       release,
       medium,
       medium_format
where  artist.name = 'The Beatles'
  and artist.id = artist_credit_name.artist
  and artist_credit_name.artist_credit = release.artist_credit
  and release_info.date_year >= artist.begin_date_year
  and release_info.date_year <= artist.end_date_year
  and release_info.release = release.id
  and area.name = 'United Kingdom'
  and area.id = release_info.area
  and release.id = medium.release
  and medium.format = medium_format.id
  and medium_format.name = '12" Vinyl'
group  by release.name
order  by RELEASE_YEAR,
          release.name;
-- Run Time (s): real 0.151 user 0.711838 sys 0.174118