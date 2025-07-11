import pandas as pd
import numpy as np
from sklearn.isotonic import IsotonicRegression
from sklearn.linear_model import LinearRegression
import matplotlib.pyplot as plt

# 1) load your historical results
hist = pd.read_csv("data/historical_projections.csv")
# columns: game_pk, expK, p_over_6.5, actual_K

# 2) Expected-K calibration (linear)
X = hist[["expK"]].values
y = hist["actual_K"].values
lin = LinearRegression().fit(X, y)
print("expK calibration:", lin.coef_[0], "× expK +", lin.intercept_)

# plot
plt.scatter(hist["expK"], hist["actual_K"], s=10)
xs = np.linspace(hist["expK"].min(), hist["expK"].max(), 100)
plt.plot(xs, lin.predict(xs.reshape(-1,1)), label="fit")
plt.xlabel("predicted expK"); plt.ylabel("actual K")
plt.legend(); plt.show()

# 3) Prob calibration (isotonic)
ir = IsotonicRegression(out_of_bounds="clip")
ir.fit(hist["p_over_6.5"], (hist["actual_K"] >= 6.5).astype(int))

# plot
prob_true, prob_pred = calibration_curve(
    (hist["actual_K"] >= 6.5).astype(int),
    hist["p_over_6.5"], n_bins=10
)
plt.plot(prob_pred, prob_true, marker="o")
plt.plot([0,1],[0,1], "--")
plt.xlabel("predicted P(K≥6.5)"); plt.ylabel("observed frequency")
plt.show()

# 4) Save calibrators for production
import pickle
with open("models/expK_lin.pkl","wb") as f: pickle.dump(lin, f)
with open("models/p_over_cal.pkl","wb") as f: pickle.dump(ir, f)
