function toggleUsernameField() {
    $("#save-btn").toggleClass("d-none");
    $("#edit-btn").toggleClass("d-none");
    $("#username-input").toggleClass("d-none");
    $("#username-readonly").toggleClass("d-none");
}

function saveUsername(newUsername) {

    $.ajax({
        url: "/update-username/",
        type: "POST",
        headers: {
            "X-CSRFToken": Cookies.get("csrftoken")
        },
        data: {
            "username": newUsername
        }
    });
}