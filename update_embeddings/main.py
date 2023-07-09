from db.update_table import insert_embedding, get_embeddings, get_relevant_data
from db.config import Session
from datetime import datetime
from tqdm import tqdm

if __name__ == "__main__":
    with Session() as session:
        data = get_relevant_data(session)
        for d in tqdm(data):
            uuid = d[0]
            text = d[1]
            article_ids = d[2]
            if article_ids is not None:
                article_ids = ",".join(article_ids)
            embedding_dict = get_embeddings(text)
            embedding = embedding_dict["embedding"]
            model_name = embedding_dict["model"]
            date_time = datetime.now()
            insert_embedding(
                session, uuid, article_ids, embedding, model_name, date_time
            )
        session.commit()
