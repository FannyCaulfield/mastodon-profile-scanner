#!/usr/bin/env python3
import sys
import json
import os
import requests
import time
from datetime import datetime
from mastodon import Mastodon, MastodonRatelimitError, MastodonAPIError
from tqdm import tqdm
import traceback

def monitor_rate_limit(mastodon):
    """Surveille les limites d'API et retourne un message si on est en attente"""
    remaining = mastodon.ratelimit_remaining
    limit = mastodon.ratelimit_limit
    
    if remaining is not None and limit is not None:
        ratio = remaining / limit
        if ratio < 0.1:  # Moins de 10% de requêtes restantes
            reset_time = mastodon.ratelimit_reset - time.time()
            if reset_time > 0:
                return f"⚠️ Limite d'API proche : {remaining}/{limit} requêtes restantes. Réinitialisation dans {reset_time:.0f} secondes."
    
    return None

def download_media(url, save_path):
    """Télécharge un fichier média depuis une URL"""
    try:
        response = requests.get(url, stream=True)
        response.raise_for_status()
        
        with open(save_path, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
        
        return True
    except Exception as e:
        print(f"⚠️ Erreur lors du téléchargement de {url}: {str(e)}")
        return False

def get_context_for_status(mastodon, status_id):
    """Récupère le contexte (réponses) d'un statut"""
    try:
        context = mastodon.status_context(status_id)
        
        # Traitement des descendants (réponses)
        replies = []
        for reply in context.descendants:
            reply_data = {
                "id": reply.id,
                "account_id": reply.account.id,
                "account_username": reply.account.username,
                "account_display_name": reply.account.display_name,
                "content": reply.content,
                "created_at": str(reply.created_at)
            }
            replies.append(reply_data)
        
        return replies
    except Exception as e:
        print(f"⚠️ Erreur lors de la récupération du contexte pour le statut {status_id}: {str(e)}")
        return []

def process_media_attachments(media_attachments):
    """Traite les pièces jointes média d'un statut"""
    processed_media = []
    
    for media in media_attachments:
        # Vérifier si c'est déjà un dictionnaire
        if isinstance(media, dict):
            media_data = media.copy()
        else:
            # Si c'est un objet Mastodon, le convertir en dictionnaire
            media_data = {
                "id": media.id,
                "type": media.type,
                "url": media.url,
                "preview_url": media.preview_url,
                "description": media.description
            }
            
            # Ajout des propriétés spécifiques selon le type de média
            if media.type == "image":
                media_data["meta"] = media.meta if hasattr(media, 'meta') else {}
            elif media.type == "video":
                media_data["meta"] = media.meta if hasattr(media, 'meta') else {}
        
        processed_media.append(media_data)
    return processed_media

def extract_instance_from_url(url):
    """Extrait le nom de l'instance à partir de l'URL"""
    try:
        from urllib.parse import urlparse
        parsed_url = urlparse(url)
        return parsed_url.netloc
    except:
        return None

def get_followers_or_following_direct(mastodon, account_id, type_list="followers"):
    """Récupère tous les followers ou following directement via des requêtes HTTP"""
    items = []
    api_base_url = mastodon.api_base_url
    endpoint = f"/api/v1/accounts/{account_id}/{type_list}"
    url = f"{api_base_url}{endpoint}"
    
    # Récupérer le nombre total attendu
    account = mastodon.account(account_id)
    expected_total = account.followers_count if type_list == "followers" else account.following_count
    
    with tqdm(desc=f"Récupération des {type_list}", total=expected_total, unit=" items") as pbar:
        params = {"limit": 80}
        headers = {"Authorization": f"Bearer {mastodon.access_token}"}
        
        while True:
            # Vérification des limites d'API
            rate_limit_msg = monitor_rate_limit(mastodon)
            if rate_limit_msg:
                pbar.write(rate_limit_msg)
            
            try:
                response = requests.get(url, params=params, headers=headers)
                response.raise_for_status()
                
                batch = response.json()
                if not batch:
                    break
                
                # Filtrer les doublons potentiels
                new_items = [item for item in batch if item["id"] not in [existing["id"] for existing in items]]
                if not new_items:
                    break
                
                items.extend(new_items)
                pbar.update(len(new_items))
                
                # Extraire le lien pour la page suivante à partir de l'en-tête Link
                if "Link" in response.headers:
                    links = response.headers["Link"].split(",")
                    next_link = None
                    
                    for link in links:
                        if 'rel="next"' in link:
                            next_link = link.split(";")[0].strip("<>")
                            break
                    
                    if next_link:
                        # Extraire les paramètres de la prochaine requête
                        from urllib.parse import urlparse, parse_qs
                        parsed_url = urlparse(next_link)
                        params = parse_qs(parsed_url.query)
                        # Convertir les listes en valeurs simples
                        params = {k: v[0] for k, v in params.items()}
                    else:
                        break
                else:
                    break
                
                # Si on a atteint le nombre total attendu, on s'arrête
                if len(items) >= expected_total:
                    break
                    
                # Pause pour éviter de surcharger l'API
                time.sleep(0.5)
                
            except requests.exceptions.RequestException as e:
                pbar.write(f"⚠️ Erreur lors de la requête : {str(e)}")
                # Si c'est une erreur de limite de débit, attendre
                if hasattr(response, 'status_code') and response.status_code == 429:
                    retry_after = int(response.headers.get("Retry-After", 60))
                    pbar.write(f"⏳ Limite d'API atteinte. En attente pour {retry_after} secondes...")
                    time.sleep(retry_after)
                else:
                    break
    
    return items

def get_all_items(fetch_function, account_id, mastodon, desc="items"):
    """Récupère tous les éléments en utilisant la pagination"""
    items = []
    expected_total = None
    since_id = None
    
    with tqdm(desc=f"Récupération des {desc}", unit=" items") as pbar:
        if desc in ["followers", "following"]:
            # Pour followers/following, on récupère d'abord le nombre total attendu
            account = mastodon.account(account_id)
            expected_total = account.followers_count if desc == "followers" else account.following_count
            pbar.total = expected_total
            pbar.refresh()
        
        while True:
            # Vérification des limites d'API
            rate_limit_msg = monitor_rate_limit(mastodon)
            if rate_limit_msg:
                pbar.write(rate_limit_msg)
            
            try:
                params = {
                    "limit": 80 if desc in ["followers", "following"] else 40
                }
                
                if since_id:
                    params["since_id"] = since_id
                
                batch = fetch_function(account_id, **params)
                if not batch:
                    break
                
                # Trier les éléments par ID pour assurer l'ordre chronologique
                batch = sorted(batch, key=lambda x: int(x.id))
                
                # Filtrer les doublons potentiels
                new_items = [item for item in batch if item.id not in [existing.id for existing in items]]
                if not new_items:
                    break
                
                items.extend(new_items)
                pbar.update(len(new_items))
                
                # Mettre à jour since_id pour la pagination
                since_id = batch[-1].id
                
                # Si on a atteint le nombre total attendu, on s'arrête
                if desc in ["followers", "following"] and len(items) >= expected_total:
                    break
                
            except MastodonRatelimitError as e:
                pbar.write(f"⏳ Limite d'API atteinte. En attente pour {e.reset_in:.0f} secondes...")
                time.sleep(e.reset_in)
    
    return items

def save_data_to_file(data, filepath, mode="w"):
    """Sauvegarde des données dans un fichier JSON"""
    try:
        with open(filepath, mode, encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        return True
    except Exception as e:
        print(f"❌ Erreur lors de la sauvegarde dans {filepath}: {str(e)}")
        return False

def clean_content(content):
    """Nettoie le contenu HTML d'un post pour extraire le texte pur et les hashtags"""
    import re
    from bs4 import BeautifulSoup

    # Créer un parseur BeautifulSoup
    soup = BeautifulSoup(content, 'html.parser')
    
    # Extraire tous les hashtags
    hashtags = []
    for tag in soup.find_all('a', class_='mention hashtag'):
        hashtag = tag.find('span').text
        if hashtag:
            hashtags.append(hashtag)
    
    # Extraire les mentions
    mentions = []
    for mention in soup.find_all('a', class_='u-url mention'):
        username = mention.find('span')
        if username:
            mentions.append(username.text)
    
    # Extraire les URLs
    urls = []
    for link in soup.find_all('a'):
        if not ('hashtag' in link.get('class', []) or 'mention' in link.get('class', [])):
            url = link.get('href')
            if url:
                urls.append(url)
    
    # Nettoyer le texte
    # Remplacer les balises <br> par des sauts de ligne
    for br in soup.find_all('br'):
        br.replace_with('\n')
    
    # Obtenir le texte pur
    text = soup.get_text(separator=' ').strip()
    
    return {
        "text": text,
        "hashtags": hashtags,
        "mentions": mentions,
        "urls": urls
    }

def get_all_posts(mastodon, account_id, max_count=None):
    """Récupère tous les posts d'un compte avec pagination complète"""
    all_posts = []
    max_id = None
    
    # Déterminer le nombre total de posts
    account = mastodon.account(account_id)
    total_posts = account.statuses_count
    
    with tqdm(desc=f"Récupération des posts", total=total_posts, unit=" posts") as pbar:
        while True:
            # Vérification des limites d'API
            rate_limit_msg = monitor_rate_limit(mastodon)
            if rate_limit_msg:
                pbar.write(rate_limit_msg)
            
            try:
                # Paramètres de requête
                params = {"limit": 40}
                if max_id:
                    params["max_id"] = max_id
                
                # Récupération du batch de posts
                batch = mastodon.account_statuses(account_id, **params)
                if not batch:
                    break
                
                # Mise à jour de max_id pour la pagination
                max_id = batch[-1].id
                
                # Filtrer les doublons potentiels
                new_posts = [post for post in batch if post.id not in [existing["id"] for existing in all_posts]]
                if not new_posts:
                    break
                
                # Traiter chaque post pour ajouter des informations supplémentaires
                processed_posts = []
                for post in new_posts:
                    post_dict = {
                        "id": post.id,
                        "created_at": str(post.created_at),
                        "type": "reblog" if hasattr(post, 'reblog') and post.reblog else "favorite" if not post.content and "/activity" in post.url else "original",
                        "url": post.url,
                        "reblogs_by": post.reblogs_count,
                        "favourites_count": post.favourites_count,
                        "replies_count": post.replies_count,
                        "media_attachments": process_media_attachments(post.media_attachments),
                        "in_reply_to_id": post.in_reply_to_id,
                        "in_reply_to_account_id": post.in_reply_to_account_id,
                        "language": post.language,
                        "visibility": post.visibility,
                        "sensitive": post.sensitive,
                        "spoiler_text": post.spoiler_text,
                        "poll": post.poll if hasattr(post, 'poll') else None
                    }

                    # Récupérer les reblogs et favoris si le post est public
                    if post.visibility == "public":
                        try:
                            rebloggers = mastodon.status_reblogged_by(post.id)
                            post_dict["rebloggers"] = []
                            for user in rebloggers:
                                try:
                                    # Extraire le username et l'instance de l'URL
                                    acct = f"{user.username}@{extract_instance_from_url(user.url)}"
                                    # Récupérer les infos complètes du compte
                                    account = mastodon.account_lookup(acct)
                                    post_dict["rebloggers"].append({
                                        "id": account.id,
                                        "username": account.username,
                                        "display_name": account.display_name,
                                        "url": account.url,
                                        "instance": extract_instance_from_url(account.url),
                                        "avatar": account.avatar,
                                        "bot": account.bot,
                                        "created_at": str(account.created_at),
                                        "followers_count": account.followers_count,
                                        "following_count": account.following_count,
                                        "statuses_count": account.statuses_count
                                    })
                                except:
                                    # En cas d'erreur, on garde les infos de base
                                    post_dict["rebloggers"].append({
                                        "id": user.id if hasattr(user, 'id') else None,
                                        "username": user.username,
                                        "display_name": user.display_name,
                                        "url": user.url,
                                        "instance": extract_instance_from_url(user.url)
                                    })
                        except:
                            post_dict["rebloggers"] = []

                        try:
                            favouriters = mastodon.status_favourited_by(post.id)
                            post_dict["favouriters"] = []
                            for user in favouriters:
                                try:
                                    # Extraire le username et l'instance de l'URL
                                    acct = f"{user.username}@{extract_instance_from_url(user.url)}"
                                    # Récupérer les infos complètes du compte
                                    account = mastodon.account_lookup(acct)
                                    post_dict["favouriters"].append({
                                        "id": account.id,
                                        "username": account.username,
                                        "display_name": account.display_name,
                                        "url": account.url,
                                        "instance": extract_instance_from_url(account.url),
                                        "avatar": account.avatar,
                                        "bot": account.bot,
                                        "created_at": str(account.created_at),
                                        "followers_count": account.followers_count,
                                        "following_count": account.following_count,
                                        "statuses_count": account.statuses_count
                                    })
                                except:
                                    # En cas d'erreur, on garde les infos de base
                                    post_dict["favouriters"].append({
                                        "id": user.id if hasattr(user, 'id') else None,
                                        "username": user.username,
                                        "display_name": user.display_name,
                                        "url": user.url,
                                        "instance": extract_instance_from_url(user.url)
                                    })
                        except:
                            post_dict["favouriters"] = []
                    else:
                        post_dict["rebloggers"] = []
                        post_dict["favouriters"] = []

                    # Traiter le contenu selon le type de post
                    if post_dict["type"] == "reblog" and post.reblog:
                        reblog_content = clean_content(post.reblog.content)
                        post_dict["content"] = reblog_content
                        post_dict["reblogged_from"] = {
                            "id": post.reblog.id,
                            "account": {
                                "username": post.reblog.account.username,
                                "display_name": post.reblog.account.display_name,
                                "url": post.reblog.account.url
                            },
                            "created_at": str(post.reblog.created_at),
                            "media_attachments": process_media_attachments(post.reblog.media_attachments)
                        }
                    elif post_dict["type"] == "favorite":
                        try:
                            status_id = post.url.split('/')[-2]
                            original_status = mastodon.status(status_id)
                            fav_content = clean_content(original_status.content)
                            post_dict["content"] = fav_content
                            post_dict["favorited_status"] = {
                                "id": original_status.id,
                                "account": {
                                    "username": original_status.account.username,
                                    "display_name": original_status.account.display_name,
                                    "url": original_status.account.url
                                },
                                "created_at": str(original_status.created_at)
                            }
                        except:
                            post_dict["content"] = {"text": "Action non disponible", "hashtags": [], "mentions": [], "urls": []}
                    else:
                        post_dict["content"] = clean_content(post.content)

                    processed_posts.append(post_dict)
                
                all_posts.extend(processed_posts)
                pbar.update(len(processed_posts))
                
                # Si on a atteint le nombre maximum demandé
                if max_count and len(all_posts) >= max_count:
                    all_posts = all_posts[:max_count]
                    break
                
                # Pause pour éviter de surcharger l'API
                time.sleep(0.5)
                
            except MastodonRatelimitError as e:
                pbar.write(f"⏳ Limite d'API atteinte. En attente pour {e.reset_in:.0f} secondes...")
                time.sleep(e.reset_in)
    
    return all_posts

def get_profile_info(profile_arg, download_media_files=True):
    """Récupère les informations d'un profil Mastodon"""
    # Séparation du nom d'utilisateur et de l'instance
    try:
        if "@" in profile_arg:
            username, instance = profile_arg.split("@")
        else:
            username = profile_arg
            instance = "mastodon.social"  # Instance par défaut
    except ValueError:
        print("❌ Format de profil invalide. Utilisez 'username@instance'.")
        return None

    # Création du dossier de sortie
    output_dir = f"scrapped_precise_{username}_{instance}"
    os.makedirs(output_dir, exist_ok=True)
    
    # Création du dossier pour les médias
    media_dir = os.path.join(output_dir, "media")
    os.makedirs(media_dir, exist_ok=True)
    
    # Chemins des fichiers de sortie
    profile_file = os.path.join(output_dir, "profile_info.json")
    posts_file = os.path.join(output_dir, "posts.json")
    followers_file = os.path.join(output_dir, "followers.json")
    following_file = os.path.join(output_dir, "following.json")

    try:
        # Connexion à l'API Mastodon
        print(f"🔄 Connexion à l'API Mastodon pour {username}@{instance}...")
        mastodon = Mastodon(
            api_base_url=f"https://{instance}",
            ratelimit_method="pace"
        )
        
        # Récupération des informations du compte
        print(f"🔍 Recherche du compte {username}...")
        account = mastodon.account_lookup(f"{username}@{instance}")
        
        # Informations de base du profil
        print(f"✅ Compte trouvé : {account.display_name} (@{account.username})")
        profile_info = {
            "id": account.id,
            "username": account.username,
            "display_name": account.display_name,
            "created_at": str(account.created_at) if isinstance(account.created_at, datetime) else account.created_at,
            "note": account.note,
            "url": account.url,
            "avatar": account.avatar,
            "header": account.header,
            "followers_count": account.followers_count,
            "following_count": account.following_count,
            "statuses_count": account.statuses_count,
            "last_status_at": str(account.last_status_at) if account.last_status_at else None,
            "bot": account.bot,
            "locked": account.locked,
            "fields": account.fields if hasattr(account, 'fields') else []
        }
        
        # Sauvegarde immédiate des informations du profil
        save_data_to_file(profile_info, profile_file)
        print(f"✅ Informations du profil sauvegardées dans {profile_file}")
        
        # Récupération de tous les posts avec pagination complète
        print(f"\n🔄 Récupération des posts de {username}...")
        statuses = get_all_posts(mastodon, account.id)
        
        # Traitement des statuts
        status_list = []
        
        print("\n🔄 Traitement des posts et téléchargement des médias...")
        media_count = 0
        batch_size = 20  # Nombre de posts à traiter avant sauvegarde
        
        for i, status in enumerate(tqdm(statuses, desc="Traitement des posts")):
            # Traitement des médias
            media_attachments = process_media_attachments(status["media_attachments"])
            
            # Ajouter les médias du post repartagé si présent
            if "reblogged_from" in status and status["reblogged_from"] and "media_attachments" in status["reblogged_from"]:
                media_attachments.extend(status["reblogged_from"]["media_attachments"])
            
            # Téléchargement des médias si demandé
            if download_media_files and media_attachments:
                for media in media_attachments:
                    media_url = media['url']
                    file_ext = os.path.splitext(media_url)[1]
                    if not file_ext:
                        file_ext = '.jpg' if media['type'] == 'image' else '.mp4'
                    # Utiliser l'ID du post original pour les médias repartagés
                    post_id = status["reblogged_from"]["id"] if "reblogged_from" in status and status["reblogged_from"] else status["id"]
                    save_path = os.path.join(media_dir, f"{post_id}_{media['id']}{file_ext}")
                    if download_media(media_url, save_path):
                        media['local_path'] = os.path.relpath(save_path, output_dir)
                        media_count += 1
            
            # Récupération des réponses
            replies = get_context_for_status(mastodon, status["id"])
            
            reconstructed_post = {
                "id": status["id"],
                "created_at": status["created_at"],
                "content": status["content"],
                "url": status["url"],
                "reblogs_by": status["reblogs_by"] if "reblogs_by" in status else 0,
                "favourites_count": status["favourites_count"],
                "replies_count": status["replies_count"],
                "media_attachments": media_attachments,
                "in_reply_to_id": status["in_reply_to_id"],
                "in_reply_to_account_id": status["in_reply_to_account_id"],
                "language": status["language"],
                "visibility": status["visibility"],
                "sensitive": status["sensitive"],
                "spoiler_text": status["spoiler_text"],
                "poll": status["poll"] if "poll" in status else None,
                "rebloggers": status["rebloggers"] if "rebloggers" in status else [],
                "favouriters": status["favouriters"] if "favouriters" in status else [],
                "replies": replies,
                "reblogged_from": status["reblogged_from"] if "reblogged_from" in status else None,
                "favorited_status": status["favorited_status"] if "favorited_status" in status else None
            }
            status_list.append(reconstructed_post)
            
            # Sauvegarde intermédiaire des posts
            if (i + 1) % batch_size == 0 or i == len(statuses) - 1:
                if i == 0:
                    # Premier batch, on écrit le fichier
                    save_data_to_file(status_list, posts_file)
                else:
                    # On charge d'abord le fichier existant pour ajouter les nouveaux posts
                    try:
                        with open(posts_file, 'r', encoding='utf-8') as f:
                            existing_posts = json.load(f)
                        # On ajoute seulement les nouveaux posts de ce batch
                        existing_posts.extend(status_list[-batch_size:])
                        save_data_to_file(existing_posts, posts_file)
                    except FileNotFoundError:
                        # Si le fichier n'existe pas encore
                        save_data_to_file(status_list, posts_file)
                
                print(f"✅ {len(status_list)} posts sauvegardés dans {posts_file}")

        # Récupération de tous les followers avec pagination directe
        print(f"\n🔄 Récupération des followers...")
        followers_data = get_followers_or_following_direct(mastodon, account.id, "followers")
        followers = []
        
        # Traitement et sauvegarde progressive des followers
        batch_size = 100
        for i, follower in enumerate(followers_data):
            instance = extract_instance_from_url(follower["url"])
            followers.append({
                "id": follower["id"],
                "username": follower["username"],
                "display_name": follower["display_name"],
                "url": follower["url"],
                "instance": instance,
                "avatar": follower.get("avatar"),
                "header": follower.get("header"),
                "bot": follower.get("bot", False),
                "created_at": follower.get("created_at"),
                "note": follower.get("note", ""),
                "followers_count": follower.get("followers_count", 0),
                "following_count": follower.get("following_count", 0),
                "statuses_count": follower.get("statuses_count", 0),
                "last_status_at": follower.get("last_status_at"),
                "locked": follower.get("locked", False),
                "fields": follower.get("fields", [])
            })
            
            # Sauvegarde intermédiaire des followers
            if (i + 1) % batch_size == 0 or i == len(followers_data) - 1:
                save_data_to_file(followers, followers_file)
                print(f"✅ {len(followers)} followers sauvegardés dans {followers_file}")

        # Récupération de tous les following avec pagination directe
        print(f"\n🔄 Récupération des following...")
        following_data = get_followers_or_following_direct(mastodon, account.id, "following")
        following = []
        
        # Traitement et sauvegarde progressive des following
        for i, follow in enumerate(following_data):
            instance = extract_instance_from_url(follow["url"])
            following.append({
                "id": follow["id"],
                "username": follow["username"],
                "display_name": follow["display_name"],
                "url": follow["url"],
                "instance": instance,
                "avatar": follow.get("avatar"),
                "header": follow.get("header"),
                "bot": follow.get("bot", False),
                "created_at": follow.get("created_at"),
                "note": follow.get("note", ""),
                "followers_count": follow.get("followers_count", 0),
                "following_count": follow.get("following_count", 0),
                "statuses_count": follow.get("statuses_count", 0),
                "last_status_at": follow.get("last_status_at"),
                "locked": follow.get("locked", False),
                "fields": follow.get("fields", [])
            })
            
            # Sauvegarde intermédiaire des following
            if (i + 1) % batch_size == 0 or i == len(following_data) - 1:
                save_data_to_file(following, following_file)
                print(f"✅ {len(following)} following sauvegardés dans {following_file}")
        
        print("\n✨ Scraping terminé avec succès !")
        print("📊 Résumé :")
        print(f"   - {len(status_list)} posts récupérés")
        print(f"   - {media_count} médias téléchargés")
        print(f"   - {len(followers)} followers")
        print(f"   - {len(following)} following")
        
        return {
            "profile_info": profile_info,
            "posts": status_list,
            "followers": followers,
            "following": following
        }
        
    except MastodonAPIError as e:
        print(f"❌ Erreur API Mastodon : {str(e)}")
        return None
    except Exception as e:
        print(f"❌ Erreur : {str(e)}")
        traceback.print_exc()
        return None

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python mastodonScrapper.py username@instance")
        sys.exit(1)
    
    profile_arg = sys.argv[1]
    get_profile_info(profile_arg)