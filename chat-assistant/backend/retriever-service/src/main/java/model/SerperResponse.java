package model;

import java.util.List;

public record SerperResponse(List<Item> organic) {
    public record Item(String link, String title, String snippet){}
}
