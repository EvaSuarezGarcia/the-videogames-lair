function rateGame(star, gameId, rating) {
    let ratingContainer = $(star).parent();
    let oldRating = ratingContainer.find(".fas").length;
    let steamIcon = ratingContainer.find(".fa-steam");
    let wasEstimated = steamIcon.length > 0;

    /* Fill stars */
    let stars = ratingContainer.find(".fa-star");
    stars.slice(0, rating).addClass("fas").removeClass("far");
    stars.slice(rating).addClass("far").removeClass("fas");

    /* Remove Steam markers, if any */
    stars.removeClass("text-info");
    steamIcon.fadeOut();

    /* Send rating to backend */
    $.ajax({
        url: "/rate-game/",
        type: "POST",
        headers: {
            "X-CSRFToken": Cookies.get("csrftoken")
        },
        data: {
            "game_id": gameId,
            "rating": rating
        }
    }).fail( (response) => {
        /* Undo changes */
        setTimeout(() => {
            stars.slice(0, oldRating).addClass("fas").removeClass("far");
            stars.slice(oldRating).addClass("far").removeClass("fas");
            if (wasEstimated) {
                stars.addClass("text-info");
                steamIcon.fadeIn();
            }
        }, 1000);
    });
}