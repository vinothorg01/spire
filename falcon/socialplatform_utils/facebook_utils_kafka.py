import facebook
from tqdm import tqdm


class FBData:
    def __init__(self, brand_fb_settings):
        self.page_token = brand_fb_settings["token"]
        self.graph = facebook.GraphAPI(access_token=self.page_token, version="3.1")
        self.page_id = brand_fb_settings["id"]

    def get_fbposts_time_window(
        self, start_time, end_time, fields="created_time, link, message"
    ):
        posts = self.graph.get_all_connections(
            id=self.page_id,
            connection_name="published_posts",
            fields=fields,
            since=start_time,
            until=end_time,
        )
        posts_done = []
        for _, post in tqdm(enumerate(posts)):
            posts_done.append(post)

        return posts_done
