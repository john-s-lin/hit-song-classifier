# Project Description

Our research question would be the following:

_Given a list of songs with their attributes, can we correctly classify whether or not a song is a hit?_

We set out to determine by first obtaining the following data:

- Songs and their features (Artist, Album, etc.)
- Their respective ranking on the Billboard charts

From this, we will correlate the two sets of data together. We will then divide our data into training and test sets. We will build our models from the training data and then compare performance using the test split.

## Model Design

As the main objective is to classify songs based on their ranking on the Billboard Hot 100, we will be training classification models and comparing the performance of at least two algorithms.

### Class definitions

It is nearly impossible to predict a song's exact ranking on the Billboard Hot 100 chart, however, a simpler approach is to create 11 ordinal buckets that characterize the range of rankings that a song could potentially fall into. Thus the classes will be defined as follows:

| Class | Ranking                     |
| ----- | --------------------------- |
| 0     | 1-10                        |
| 1     | 11-20                       |
| 2     | 21-30                       |
| 3     | 31-40                       |
| 4     | 41-50                       |
| 5     | 51-60                       |
| 6     | 61-70                       |
| 7     | 71-80                       |
| 8     | 81-90                       |
| 9     | 91-100                      |
| 10    | Not ranked OR ranking > 100 |

This necessitates that significant data preprocessing will be required to classify songs from the dataset into the proper classes as labeled here.

### Identifying best features

As mentioned in the project clinic, identifying the features that contribute most to the models might be a good sidequest to embark on. In order to do this, techniques such as Principal Component Analysis, Pearson's Coefficient and Jaccard Similarity Score can be used to identify the top features. `sklearn` models also include `feature_importances_` as a relevant attribute to a model.

### Model evaluation

We decided to narrow our focus to compare the performance of two classification models: Random Forest Classifier (RFC) and Support Vector Machines (SVM).

#### Random Forest Classifier (RFC)

We chose the RFC model due to its relatively short training time, as RFCs are based on decision trees. However, RFCs are more immune to overfitting as it is the combined aggregate of multiple independent decision trees on the same dataset.

#### Support Vector Machines (SVM)

Despite the high performance of the SVM, its relatively longer training time make it a good target to compare against the RFC.

#### Alternative models

We are also considering other classification models such as Gradient Boost, XGBoost, and k-Nearest Neighbours (kNN).

## Data Description

- **_The musiXmatch Dataset_**: can be found at http://millionsongdataset.com/musixmatch/#desc
  - Features:
    - list of top words, in popularity order
    - track ID from MSD, track ID from musiXmatch, then word index : word count (word index starts at 1!
    - To be used with Million Song Dataset (MSD).
- **_Million Song Dataset_**: can be found at http://millionsongdataset.com/musixmatch/
  - 46 Features:
    - artist_mbid: db92a151-1ac2-438b-bc43-b82e149ddd50
      - the musicbrainz.org ID for this artists is db9...
    - artist_mbtags: shape = (4,)
      - this artist received 4 tags on musicbrainz.org
    - artist_mbtags_count: shape = (4,)
      - raw tag count of the 4 tags this artist received on musicbrainz.org
    - **_artist_name_**: Rick Astley
      - artist name
    - artist_playmeid: 1338
      - the ID of that artist on the service playme.com
    - artist_terms: shape = (12,)
      - this artist has 12 terms (tags) from The Echo Nest
    - artist_terms_freq: shape = (12,)
      - frequency of the 12 terms from The Echo Nest (number between 0 and 1)
    - artist_terms_weight: shape = (12,)
      - weight of the 12 terms from The Echo Nest (number between 0 and 1)
    - audio_md5: bf53f8113508a466cd2d3fda18b06368
      - hash code of the audio used for the analysis by The Echo Nest
    - bars_confidence: shape = (99,)
      - confidence value (between 0 and 1) associated with each bar by The Echo Nest
    - bars_start: shape = (99,)
      - start time of each bar according to The Echo Nest, this song has 99 bars
    - beats_confidence: shape = (397,)
      - confidence value (between 0 and 1) associated with each beat by The Echo Nest
    - beats_start: shape = (397,)
      - start time of each beat according to The Echo Nest, this song has 397 beats
    - **_danceability_**: 0.0
      - danceability measure of this song according to The Echo Nest (between 0 and 1, 0 => not analyzed)
    - **_duration_**: 211.69587
      - duration of the track in seconds
    - end_of_fade_in: 0.139
      - time of the end of the fade in, at the beginning of the song, according to The Echo Nest
    - **_energy_**: 0.0
      - energy measure (not in the signal processing sense) according to The Echo Nest (between 0 and 1, 0 => not analyzed)
    - key: 1
      - estimation of the key the song is in by The Echo Nest
    - key_confidence: 0.324
      - confidence of the key estimation
    - **_loudness_**: -7.75
      - general loudness of the track
    - mode: 1
      - estimation of the mode the song is in by The Echo Nest
    - mode_confidence: 0.434
      - confidence of the mode estimation
    - **_release_**: Big Tunes - Back 2 The 80s
      - album name from which the track was taken, some songs / tracks can come from many albums, we give only one
    - release_7digitalid: 786795
      - the ID of the release (album) on the service 7digital.com
    - sections_confidence: shape = (10,)
      - confidence value (between 0 and 1) associated with each section by The Echo Nest
    - sections_start: shape = (10,)
      - start time of each section according to The Echo Nest, this song has 10 sections
    - segments_confidence: shape = (935,)
      - confidence value (between 0 and 1) associated with each segment by The Echo Nest
    - segments_loudness_max: shape = (935,)
      - max loudness during each segment
    - segments_loudness_max_time: shape = (935,)
      - time of the max loudness during each segment
    - segments_loudness_start: shape = (935,)
      - loudness at the beginning of each segment
    - segments_pitches: shape = (935, 12)
      - chroma features for each segment (normalized so max is 1.)
    - segments_start: shape = (935,)
      - start time of each segment (~ musical event, or onset) according to The Echo Nest, this song has 935 segments
    - segments_timbre: shape = (935, 12)
      - MFCC-like features for each segment
    - **_similar_artists_**: shape = (100,)
      - a list of 100 artists (their Echo Nest ID) similar to Rick Astley according to The Echo Nest
    - **_song_hotttnesss_**: 0.864248830588
      - according to The Echo Nest, when downloaded (in December 2010), this song had a 'hotttnesss' of 0.8 (on a scale of 0 and 1)
    - song_id: SOCWJDB12A58A776AF
      - The Echo Nest song ID, note that a song can be associated with many tracks (with very slight audio differences)
    - start_of_fade_out: 198.536
      - start time of the fade out, in seconds, at the end of the song, according to The Echo Nest
    - tatums_confidence: shape = (794,)
      - confidence value (between 0 and 1) associated with each tatum by The Echo Nest
    - tatums_start: shape = (794,)
      - start time of each tatum according to The Echo Nest, this song has 794 tatums
    - **_tempo_**: 113.359
      - tempo in BPM according to The Echo Nest
    - **_time_signature_**: 4
      - time signature of the song according to The Echo Nest, i.e. usual number of beats per bar
    - time_signature_confidence: 0.634
      - confidence of the time signature estimation
    - **_title_**: Never Gonna Give You Up
      - song title
    - track_7digitalid: 8707738
      - the ID of this song on the service 7digital.com
    - track_id: TRAXLZU12903D05F94
      - The Echo Nest ID of this particular track on which the analysis was done
    - **_year_**: 1987
      - The year it was released
- The Billboard charts can be accessed from https://www.billboard.com/charts/. There is a python library that can be found at https://libraries.io/pypi/billboard.py to access the billboard lists. There are global and US billboard charts that can be accessed for chart data
  - Chart entry data:
    - **_title_** – The title of the track.
    - **_artist_** – The name of the artist, as formatted on Billboard.com.
    - image – The URL of the image for the track.
    - peakPos – The track's peak position on the chart as of the chart date, as an int (or None if the chart does not include this information).
    - lastPos – The track's position on the previous week's chart, as an int (or None if the chart does not include this information). This value is 0 if the track was not on the previous week's chart.
    - weeks – The number of weeks the track has been or was on the chart, including future dates (up until the present time).
    - **_rank_** – The track's current position on the chart.
    - isNew – Whether the track is new to the chart.
