import io
import os
import time

import pandas as pd
import requests
from bs4 import BeautifulSoup
from PIL import Image
from tqdm import tqdm


class ImagesDB:
    default_image_size = (250, 250)
    headers = {
        "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
        "accept-language": "en-US,en;q=0.5",
        "cache-control": "max-age=0",
        "priority": "u=0, i",
        "sec-ch-ua": '"Chromium";v="142", "Brave";v="142", "Not_A Brand";v="99"',
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": '"Windows"',
        "sec-fetch-dest": "document",
        "sec-fetch-mode": "navigate",
        "sec-fetch-site": "cross-site",
        "sec-fetch-user": "?1",
        "sec-gpc": "1",
        "upgrade-insecure-requests": "1",
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/142.0.0.0 Safari/537.36",
    }

    drivers_subdirectory = "drivers"
    constructors_subdirectory = "constructors"

    def __init__(self, directory: str) -> None:
        self.directory = directory

    def _get_image_url_from_wikipedia(self, url: str) -> str:
        response = requests.get(url, headers=self.headers)
        soup = BeautifulSoup(response.text, features="html.parser")
        try:
            img_url = "https:" + soup.find(class_="infobox-image").find("img")["src"]
        except AttributeError:
            img_url = None
        return img_url

    def _get_image_url_from_seeklogo(self, name: str) -> str | None:
        params = {"q": name}
        response = requests.get("https://seeklogo.com/search", params=params, headers=self.headers)
        soup = BeautifulSoup(response.content, "html.parser")
        img = soup.select_one("ul.logoGroupCt img.logoImage")
        return img["src"] if img and img.get("src") else None

    def _format_image(self, image: Image.Image) -> Image.Image:
        width, height = image.size

        side = min(width, height)
        left = (width - side) // 2
        top = (height - side) // 2
        right = left + side
        bottom = top + side

        image_crop = image.crop((left, top, right, bottom))

        image_resized = image_crop.resize(self.default_image_size, Image.LANCZOS)

        return image_resized

    def _save_image(self, image: Image.Image, id: str, subdirectory: str) -> None:
        dir_path = os.path.join(self.directory, subdirectory)
        os.makedirs(dir_path, exist_ok=True)
        image_path = os.path.join(dir_path, f"{id}.png")
        image.save(image_path, format="PNG")

    def _download_image(self, url: str) -> Image.Image | None:
        for _ in range(5):
            try:
                response = requests.get(url, headers=self.headers)
                if response.status_code == 200:
                    break
            except requests.exceptions.RequestException:
                pass
            time.sleep(3)
        if response.status_code != 200:
            print(f"\tError downloading image for driver {id}: {url}")
            return
        image = Image.open(io.BytesIO(response.content))
        return image

    def _get_constructors_urls(self, constructors: pd.DataFrame) -> dict:
        constructors_images = {}
        existing_ids = set([x.replace(".png", "") for x in os.listdir(os.path.join(self.directory, self.constructors_subdirectory))])
        constructors = constructors[~constructors["constructorId"].isin(existing_ids)].copy()
        for _, constructor in tqdm(constructors.iterrows(), total=constructors.shape[0], desc="Urls constructors"):
            img_url = self._get_image_url_from_seeklogo(constructor.name)
            if img_url is not None:
                constructors_images[constructor.constructorId] = img_url
        return constructors_images

    def update_images_constructors(self, constructors: pd.DataFrame):
        constructors_images_urls = self._get_constructors_urls(constructors)
        for constructor_id, img_url in tqdm(constructors_images_urls.items(), desc="Downloading constructors images"):
            image = self._download_image(img_url)
            if image is None:
                continue
            image_formatted = self._format_image(image)
            self._save_image(image_formatted, constructor_id, self.constructors_subdirectory)

    def _get_driver_images_urls(self, drivers: pd.DataFrame) -> dict:
        drivers_images = {}
        existing_ids = set([x.replace(".png", "") for x in os.listdir(os.path.join(self.directory, self.drivers_subdirectory))])
        drivers = drivers[~drivers["driverId"].isin(existing_ids)].copy()
        for _, driver in tqdm(drivers.iterrows(), total=drivers.shape[0], desc="Urls drivers"):
            img_url = self._get_image_url_from_wikipedia(driver.url)
            if img_url is not None:
                drivers_images[driver.driverId] = img_url
        return drivers_images

    def update_images_drivers(self, drivers: pd.DataFrame):
        drivers_images_urls = self._get_driver_images_urls(drivers)
        for driver_id, img_url in tqdm(drivers_images_urls.items(), desc="Downloading drivers images"):
            image = self._download_image(img_url)
            if image is None:
                continue
            image_formatted = self._format_image(image)
            self._save_image(image_formatted, driver_id, self.drivers_subdirectory)
