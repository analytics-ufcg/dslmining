package com.example.libimset;

import org.apache.mahout.cf.taste.common.Refreshable;
import org.apache.mahout.cf.taste.common.TasteException;
import org.apache.mahout.cf.taste.impl.common.FastIDSet;
import org.apache.mahout.cf.taste.impl.model.file.FileDataModel;
import org.apache.mahout.cf.taste.impl.neighborhood.NearestNUserNeighborhood;
import org.apache.mahout.cf.taste.impl.recommender.GenericUserBasedRecommender;
import org.apache.mahout.cf.taste.impl.recommender.svd.ALSWRFactorizer;
import org.apache.mahout.cf.taste.impl.recommender.svd.SVDRecommender;
import org.apache.mahout.cf.taste.impl.similarity.EuclideanDistanceSimilarity;
import org.apache.mahout.cf.taste.model.DataModel;
import org.apache.mahout.cf.taste.recommender.IDRescorer;
import org.apache.mahout.cf.taste.recommender.RecommendedItem;
import org.apache.mahout.cf.taste.recommender.Recommender;
import org.apache.mahout.cf.taste.similarity.UserSimilarity;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.List;

import static com.example.grouplens.GroupLensDataModel.readResourceToTempFile;
import static com.example.libimset.Utils.parseMenWomen;

/**
 * Created by igleson on 06/02/15.
 */
public class LibimsetRecommender implements Recommender {

    private final DataModel model;
    private final UserSimilarity similarity;
    private final NearestNUserNeighborhood neighborhood;
    private final Recommender delegate;
    private final FastIDSet men;
    private final FastIDSet women;
    private final FastIDSet usersRateMoreMen;
    private final FastIDSet usersRateLessMen;

    public LibimsetRecommender(String ratingsPath, String gendersPath) throws IOException, TasteException {
        this(new FileDataModel(new File(ratingsPath)), gendersPath);
    }

    public LibimsetRecommender(DataModel model, String genderPath) throws TasteException, IOException {
        this.model = model;
        this.similarity = new EuclideanDistanceSimilarity(model);
        this.neighborhood = new NearestNUserNeighborhood(2, similarity, model);
//        this.delegate = new SVDRecommender(model, new ALSWRFactorizer(model, 10, 0.05, 10)); //2.6959293545658816
        this.delegate = new GenericUserBasedRecommender(model, neighborhood, similarity); //0.898494110535326
        FastIDSet[] menAndWomen = parseMenWomen(genderPath);
        this.men = menAndWomen[0];
        this.women = menAndWomen[1];
        this.usersRateMoreMen = new FastIDSet(50000);
        this.usersRateLessMen = new FastIDSet(50000);
    }

    @Override
    public List<RecommendedItem> recommend(long userID, int howMany) throws TasteException {
        return delegate.recommend(userID, howMany, new GenderRescorer(men, women, usersRateMoreMen, usersRateLessMen, userID, model));
    }

    @Override
    public List<RecommendedItem> recommend(long userID, int howMany, IDRescorer idRescorer) throws TasteException {
        return delegate.recommend(userID, howMany, idRescorer);
    }

    @Override
    public float estimatePreference(long userID, long itemID) throws TasteException {
        IDRescorer rescorer = new GenderRescorer(men, women, usersRateMoreMen, usersRateLessMen, userID, model);
        return (float) rescorer.rescore(itemID, delegate.estimatePreference(userID, itemID));
    }

    @Override
    public void setPreference(long userID, long itemID, float value)
            throws TasteException {
        delegate.setPreference(userID, itemID, value);
    }

    @Override
    public void removePreference(long userID, long itemID)
            throws TasteException {
        delegate.removePreference(userID, itemID);
    }

    @Override
    public DataModel getDataModel() {
        return delegate.getDataModel();
    }

    @Override
    public void refresh(Collection<Refreshable> alreadyRefreshed) {
        delegate.refresh(alreadyRefreshed);
    }
}
