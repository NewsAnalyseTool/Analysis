package model

// define the model so that it is read from json and converted into an object
case class RedditPost(
    category: String,
    title: String,
    content: String,
    date: String,
    isReddit: Boolean,
    upvotes: Int,
    comments: Int
)

