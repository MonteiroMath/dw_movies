'''
- IMDbMovies
  - Title
  - Summary
  - Director
  - writer
  - main genres
  - adult rating
  - release year
  - runtime
  - budget
  - grossUSCA
  - grossWorld
  - openingUSCA
  - openingWorld


- title-basics
  - tconst (id)
  - titleType
  - primaryTitle
  - originalTitle
  - runtime
  - genres

  -> filter movie and tvMovie, keep all data

- title-crew
  - tconst
  - directors
  - writers

  -> keep tconst and directors

- title-principals
  - People that worked in the movie. 
  -> Filter with category = actor and pick tconst and nconst?

- title-ratings
  - tconst
  - averageRating
  - numVotes

  -> keep all columns, use tconst to merge

- names-basic
  - nconst (id)
  - primary name
  - birthYear
  - deathYear
  - primaryProfession, know ForTitles (discard?)

  - maybe use the tcelebs instead ffor actors? More personal data

- actors (TMDB - celebs)

  - lots of data
  -> get name, birthday, deathday, gender (1-female, 2-male), place_of_birth, discard the rest
  -> how to merge with the others? using name as the merge prop and names-basic as a bridge? Will get very messy

- directors
  - name
  - date of birth
  - date of death
  - place of birth
  - gender

  -> merge by name?



'''