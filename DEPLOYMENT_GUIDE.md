# 🚀 Deploy Your Portfolio to GitHub Pages

## Quick Start - Get Your Portfolio Live in 5 Minutes!

### Step 1: Initialize Git Repository

Open Terminal and run these commands:

```bash
cd /Users/sohanakadiwala/Desktop/Demoantigravity

# Initialize git
git init

# Add all files
git add .

# Create first commit
git commit -m "Initial commit: Data Engineering Portfolio with Snowflake ETL project"
```

### Step 2: Create GitHub Repository

1. Go to [GitHub.com](https://github.com) and sign in
2. Click the **"+"** icon (top right) → **"New repository"**
3. Repository settings:
   - **Name**: `data-engineering-portfolio` (or any name you prefer)
   - **Description**: "Data Engineering Portfolio showcasing ETL pipelines and cloud data projects"
   - **Public** (must be public for free GitHub Pages)
   - ❌ **Do NOT** initialize with README (we already have files)
4. Click **"Create repository"**

### Step 3: Connect and Push to GitHub

GitHub will show you commands. Copy and run them in Terminal:

```bash
# Add GitHub as remote (replace YOUR_USERNAME with your GitHub username)
git remote add origin https://github.com/YOUR_USERNAME/data-engineering-portfolio.git

# Rename branch to main (if needed)
git branch -M main

# Push to GitHub
git push -u origin main
```

### Step 4: Enable GitHub Pages

1. In your GitHub repository, click **"Settings"** (top menu)
2. Scroll down to **"Pages"** (left sidebar)
3. Under **"Source"**:
   - Select branch: **main**
   - Select folder: **/ (root)**
4. Click **"Save"**

### Step 5: Get Your Public URL! 🎉

After 1-2 minutes, your portfolio will be live at:

```
https://YOUR_USERNAME.github.io/data-engineering-portfolio/
```

**Your Snowflake project showcase will be at:**
```
https://YOUR_USERNAME.github.io/data-engineering-portfolio/projects/snowflake-etl-pipeline/
```

---

## Alternative: Even Faster with Netlify (Drag & Drop)

### Option A: Netlify Drop (No Git Required)

1. Go to [app.netlify.com/drop](https://app.netlify.com/drop)
2. Sign up/login (free account)
3. **Drag the entire `Demoantigravity` folder** into the browser
4. Get instant public URL like: `https://your-site-name.netlify.app`

### Option B: Netlify with Git (Recommended for Updates)

1. Push to GitHub (Steps 1-3 above)
2. Go to [netlify.com](https://netlify.com) → Sign up
3. Click **"Add new site"** → **"Import an existing project"**
4. Connect GitHub → Select your repository
5. Deploy settings:
   - Build command: (leave empty)
   - Publish directory: `/`
6. Click **"Deploy"**

**Advantages:**
- ✅ Instant deploys (faster than GitHub Pages)
- ✅ Custom domain support
- ✅ Automatic HTTPS
- ✅ Deploy previews for changes

---

## Alternative: Vercel (Also Free & Fast)

1. Push to GitHub (Steps 1-3 above)
2. Go to [vercel.com](https://vercel.com) → Sign up
3. Click **"New Project"**
4. Import your GitHub repository
5. Click **"Deploy"**

Get URL like: `https://your-portfolio.vercel.app`

---

## 📝 After Deployment - Update Your Resume

Once live, add these links:

### Resume:
```
Portfolio: https://YOUR_USERNAME.github.io/data-engineering-portfolio/
GitHub: https://github.com/YOUR_USERNAME/data-engineering-portfolio
```

### LinkedIn:
1. Add to **"Featured"** section
2. Update headline: "Data Engineer | Snowflake | Python | ETL Pipelines"
3. Add portfolio link to **"Contact Info"**

### Cover Letters:
```
"View my production-ready Snowflake ETL pipeline at: 
https://YOUR_USERNAME.github.io/data-engineering-portfolio/projects/snowflake-etl-pipeline/"
```

---

## 🔄 How to Update Your Portfolio Later

After making changes to your files:

```bash
cd /Users/sohanakadiwala/Desktop/Demoantigravity

# Stage changes
git add .

# Commit with message
git commit -m "Updated project descriptions"

# Push to GitHub
git push

# GitHub Pages will auto-update in 1-2 minutes!
```

---

## ✅ Recommended: GitHub Pages (Best for Portfolios)

**Why GitHub Pages:**
- ✅ Free forever
- ✅ Custom domain support (yourdomain.com)
- ✅ Shows your GitHub activity
- ✅ Recruiters can see your code
- ✅ Professional URL

**Your final URLs will be:**
- Portfolio: `https://YOUR_USERNAME.github.io/data-engineering-portfolio/`
- Snowflake Project: `https://YOUR_USERNAME.github.io/data-engineering-portfolio/projects/snowflake-etl-pipeline/`

---

## 🆘 Troubleshooting

**Issue: "git: command not found"**
```bash
# Install Git
xcode-select --install
```

**Issue: GitHub asks for password**
- Use a Personal Access Token instead
- Go to GitHub → Settings → Developer settings → Personal access tokens
- Generate new token with "repo" permissions

**Issue: Page shows 404**
- Wait 2-3 minutes after enabling GitHub Pages
- Check Settings → Pages to see deployment status
- Make sure `index.html` is in the root directory ✅ (it is!)

---

## 🎯 Next Steps

1. ✅ Follow Step 1-5 above to deploy
2. ✅ Test your public URL
3. ✅ Add URL to resume and LinkedIn
4. ✅ Share with recruiters!

**Need help? Let me know which deployment method you'd like to use!**
