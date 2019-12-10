package dto;

import java.util.List;

public class ReviewAnalysisDto {
    private boolean isSarcastic;
    private int sentiment;
    private List<String> namedEntities;

    public int getSentiment() {
        return sentiment;
    }

    public void setSentiment(int sentiment) {
        this.sentiment = sentiment;
    }

    public List<String> getNamedEntities() {
        return namedEntities;
    }

    public void setNamedEntities(List<String> namedEntities) {
        this.namedEntities = namedEntities;
    }

    public ReviewAnalysisDto(boolean isSarcastic, int sentiment, List<String> namedEntitites) {
        this.isSarcastic = isSarcastic;
        this.sentiment = sentiment;
        this.namedEntities = namedEntitites;
    }

    public boolean isSarcastic() {
        return isSarcastic;
    }

    public void setSarcastic(boolean sarcastic) {
        isSarcastic = sarcastic;
    }
}
