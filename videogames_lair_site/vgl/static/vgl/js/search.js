function toggleAdvancedFilters(button) {
    $('#advanced-filters').toggleClass(["collapsed"]);
    $(button).find(".fas").toggleClass("fa-rotate-180");
}

$(document).ready(function () {
    $("form").submit(function () {
        $(".advanced-search:not([type=hidden])").remove();
    });

    $(".js-range-slider").ionRangeSlider({
        type: "double",
        skin: "round"
    });

    $(".basic-autocomplete").autoComplete({minLength: 1, preventEnter: true})
        .on("autocomplete.select", function (event, value) {
            let hidden_input = this.cloneNode();
            hidden_input.type = "hidden";
            hidden_input.value = value;

            let span = document.createElement("span");
            span.innerText = value;
            span.classList.add("bg-info", "text-white", "rounded-pill", "px-2", "mt-1", "mr-1", "d-inline-block");
            span.appendChild(hidden_input);

            let close = document.createElement("button");
            close.type = "button";
            close.classList.add("close", "text-white", "ml-1", "line-height-inherit", "font-weight-inherit",
                "font-size-inherit");
            close.setAttribute("aria-label", "Close");
            let close_span = document.createElement("span");
            close_span.innerHTML = "&times;"
            close_span.setAttribute("aria-hidden", "true");
            close.appendChild(close_span);
            span.appendChild(close);

            this.parentNode.insertBefore(span, this.parentNode.lastChild.nextSibling);
            this.value = "";
    });
});