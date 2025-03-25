# mastodon-profile-scanner

usage :

``` pip install -r requirements.txt ```


``` python mastodon-profile-scanner.py [usermame]@[instance] ```
(attention sans les [] / without the [])


- creation d'un nouveau dossier scrapped_[username]_[instance] avec :
  - medias => les medias postés par le compte
  - followers.json
  - following.json
  - posts.json
  - profile_info.json

- attention : uniquement pour les profiles public

