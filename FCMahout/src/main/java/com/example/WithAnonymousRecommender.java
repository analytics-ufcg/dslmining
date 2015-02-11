package com.example;

import org.apache.mahout.cf.taste.common.Refreshable;
import org.apache.mahout.cf.taste.common.TasteException;
import org.apache.mahout.cf.taste.eval.RecommenderBuilder;
import org.apache.mahout.cf.taste.impl.model.PlusAnonymousUserDataModel;
import org.apache.mahout.cf.taste.model.DataModel;
import org.apache.mahout.cf.taste.model.PreferenceArray;
import org.apache.mahout.cf.taste.recommender.IDRescorer;
import org.apache.mahout.cf.taste.recommender.RecommendedItem;
import org.apache.mahout.cf.taste.recommender.Recommender;

import java.util.Collection;
import java.util.List;

public class WithAnonymousRecommender implements Recommender {

    private final Recommender delegate;
    private final PlusAnonymousUserDataModel model;

    public WithAnonymousRecommender(RecommenderBuilder builder, DataModel model) throws TasteException {
        this.model = new  PlusAnonymousUserDataModel(model);
        this.delegate = builder.buildRecommender(this.model);
    }

    public synchronized List<RecommendedItem> recommend(PreferenceArray anonymousUserPrefs, int howMany) throws TasteException {
       return recommend(anonymousUserPrefs, howMany, null);
    }

    public synchronized List<RecommendedItem> recommend(PreferenceArray anonymousUserPrefs, int howMany, IDRescorer idRescorer) throws TasteException {
        model.setTempPrefs(anonymousUserPrefs);
        List<RecommendedItem> recommendations = recommend(PlusAnonymousUserDataModel.TEMP_USER_ID, howMany, idRescorer);
        model.clearTempPrefs();
        return recommendations;
    }

    @Override
    public List<RecommendedItem> recommend(long userID, int howMany) throws TasteException {
        return delegate.recommend(userID, howMany);
    }

    @Override
    public List<RecommendedItem> recommend(long userID, int howMany, IDRescorer idRescorer) throws TasteException {
        return delegate.recommend(userID, howMany, idRescorer);
    }

    @Override
    public float estimatePreference(long userID, long itemID) throws TasteException {
        return delegate.estimatePreference(userID, itemID);
    }

    @Override
    public void setPreference(long userID, long itemID, float value) throws TasteException {
        delegate.setPreference(userID, itemID, value);
    }

    @Override
    public void removePreference(long userID, long itemID) throws TasteException {
        delegate.removePreference(userID, itemID);
    }

    @Override
    public DataModel getDataModel() {
        return model;
    }

    @Override
    public void refresh(Collection<Refreshable> collection) {
        delegate.refresh(collection);
    }
}
